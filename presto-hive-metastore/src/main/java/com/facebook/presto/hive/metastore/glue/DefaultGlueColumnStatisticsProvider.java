/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableRequest;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.authentication.MetastoreContext;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_NOT_FOUND;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.glue.converter.GlueStatConverter.fromGlueColumnStatistics;
import static com.facebook.presto.hive.metastore.glue.converter.GlueStatConverter.toGlueColumnStatistics;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class DefaultGlueColumnStatisticsProvider
        implements GlueColumnStatisticsProvider
{
    // Read limit for AWS Glue API GetColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetColumnStatisticsForPartition
    private static final int GLUE_COLUMN_READ_STAT_PAGE_SIZE = 100;

    // Write limit for AWS Glue API UpdateColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-UpdateColumnStatisticsForPartition
    private static final int GLUE_COLUMN_WRITE_STAT_PAGE_SIZE = 25;

    private final AWSGlueAsync glueClient;
    private final String catalogId;
    private final Executor readExecutor;
    private final Executor writeExecutor;
    private final boolean impersonationEnabled;
    private final LoadingCache<String, AWSCredentialsProvider> awsCredentialsProviderLoadingCache;
    private final Supplier<GlueSecurityMappings> mappings;

    public DefaultGlueColumnStatisticsProvider(AWSGlueAsync glueClient, String catalogId, Executor readExecutor, Executor writeExecutor, GlueHiveMetastoreConfig glueConfig,
                                               @ForGlueHiveMetastore GlueSecurityMappingsSupplier glueSecurityMappingsSupplier,
                                               LoadingCache<String, AWSCredentialsProvider> awsCredentialsProviderLoadingCache)
    {
        this.glueClient = glueClient;
        this.catalogId = catalogId;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;
        this.impersonationEnabled = glueConfig.isImpersonationEnabled();
        this.awsCredentialsProviderLoadingCache = awsCredentialsProviderLoadingCache;
        this.mappings = requireNonNull(glueSecurityMappingsSupplier, "glueSecurityMappingsSupplier is null").getMappingsSupplier();
    }

    private String getGlueIamRole(MetastoreContext metastoreContext)
    {
        GlueSecurityMapping mapping = mappings.get().getMapping(metastoreContext)
                .orElseThrow(() -> new AccessDeniedException("No matching Glue Security Mapping or Glue Security Mapping has no role"));

        return mapping.getIamRole();
    }

    private AWSCredentialsProvider getAwsCredentialsProvider(MetastoreContext metastoreContext)
    {
        String iamRole = getGlueIamRole(metastoreContext);
        return awsCredentialsProviderLoadingCache.getUnchecked(iamRole);
    }

    private AmazonWebServiceRequest updateAWSRequest(MetastoreContext metastoreContext, AmazonWebServiceRequest request)
    {
        return impersonationEnabled ? request.withRequestCredentialsProvider(getAwsCredentialsProvider(metastoreContext)) : request;
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(MetastoreContext metastoreContext, Table table)
    {
        try {
            List<String> columnNames = getAllColumns(table);
            List<List<String>> columnChunks = Lists.partition(columnNames, GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            List<CompletableFuture<GetColumnStatisticsForTableResult>> getStatsFutures = columnChunks.stream()
                    .map(partialColumns -> supplyAsync(() -> {
                        GetColumnStatisticsForTableRequest request = (GetColumnStatisticsForTableRequest) updateAWSRequest(
                                metastoreContext,
                                new GetColumnStatisticsForTableRequest()
                                    .withCatalogId(catalogId)
                                    .withDatabaseName(table.getDatabaseName())
                                    .withTableName(table.getTableName())
                                    .withColumnNames(partialColumns));
                        return glueClient.getColumnStatisticsForTable(request);
                    }, readExecutor)).collect(toImmutableList());

            HiveBasicStatistics tableStatistics = getHiveBasicStatistics(table.getParameters());
            ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
            for (CompletableFuture<GetColumnStatisticsForTableResult> future : getStatsFutures) {
                GetColumnStatisticsForTableResult tableColumnsStats = getFutureValue(future, PrestoException.class);
                for (ColumnStatistics columnStatistics : tableColumnsStats.getColumnStatisticsList()) {
                    columnStatsMapBuilder.put(
                            columnStatistics.getColumnName(),
                            fromGlueColumnStatistics(columnStatistics.getStatisticsData(), tableStatistics.getRowCount()));
                }
            }
            return columnStatsMapBuilder.build();
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private Optional<Map<String, HiveColumnStatistics>> getPartitionColumnStatisticsIfPresent(MetastoreContext metastoreContext, Partition partition)
    {
        try {
            return Optional.of(getPartitionColumnStatistics(metastoreContext, partition));
        }
        catch (PrestoException ex) {
            if (ex.getErrorCode() == HIVE_PARTITION_NOT_FOUND.toErrorCode()) {
                return Optional.empty();
            }
            throw ex;
        }
    }

    @Override
    public Map<Partition, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(MetastoreContext metastoreContext, Collection<Partition> partitions)
    {
        Map<Partition, List<CompletableFuture<GetColumnStatisticsForPartitionResult>>> resultsForPartition = new HashMap<>();
        for (Partition partition : partitions) {
            ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForPartitionResult>> futures = ImmutableList.builder();
            List<List<Column>> columnChunks = Lists.partition(partition.getColumns(), GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            for (List<Column> partialPartitionColumns : columnChunks) {
                List<String> columnsNames = partialPartitionColumns.stream()
                        .map(Column::getName)
                        .collect(toImmutableList());
                GetColumnStatisticsForPartitionRequest request = (GetColumnStatisticsForPartitionRequest) updateAWSRequest(
                        metastoreContext,
                        new GetColumnStatisticsForPartitionRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(partition.getDatabaseName())
                        .withTableName(partition.getTableName())
                        .withColumnNames(columnsNames)
                        .withPartitionValues(partition.getValues()));
                futures.add(supplyAsync(() -> glueClient.getColumnStatisticsForPartition(request), readExecutor));
            }
            resultsForPartition.put(partition, futures.build());
        }

        try {
            ImmutableMap.Builder<Partition, Map<String, HiveColumnStatistics>> partitionStatistics = ImmutableMap.builder();
            resultsForPartition.forEach((partition, futures) -> {
                HiveBasicStatistics tableStatistics = getHiveBasicStatistics(partition.getParameters());
                ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();

                for (CompletableFuture<GetColumnStatisticsForPartitionResult> getColumnStatisticsResultFuture : futures) {
                    GetColumnStatisticsForPartitionResult getColumnStatisticsResult = getFutureValue(getColumnStatisticsResultFuture);
                    getColumnStatisticsResult.getColumnStatisticsList().forEach(columnStatistics ->
                            columnStatsMapBuilder.put(
                                    columnStatistics.getColumnName(),
                                    fromGlueColumnStatistics(columnStatistics.getStatisticsData(), tableStatistics.getRowCount())));
                }

                partitionStatistics.put(partition, columnStatsMapBuilder.build());
            });

            return partitionStatistics.build();
        }
        catch (EntityNotFoundException ex) {
            throw new PrestoException(HIVE_PARTITION_NOT_FOUND, ex);
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    // Glue will accept null as min/max values but return 0 when reading
    // to avoid incorrect stats we skip writes for column statistics that have min/max null
    // this can be removed once glue fix this behaviour
    private boolean isGlueWritable(ColumnStatistics stats)
    {
        ColumnStatisticsData statisticsData = stats.getStatisticsData();
        String columnType = stats.getStatisticsData().getType();
        if (columnType.equals(ColumnStatisticsType.DATE.toString())) {
            DateColumnStatisticsData data = statisticsData.getDateColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.DECIMAL.toString())) {
            DecimalColumnStatisticsData data = statisticsData.getDecimalColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.DOUBLE.toString())) {
            DoubleColumnStatisticsData data = statisticsData.getDoubleColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.LONG.toString())) {
            LongColumnStatisticsData data = statisticsData.getLongColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        return true;
    }

    @Override
    public void updateTableColumnStatistics(MetastoreContext metastoreContext, Table table, Map<String, HiveColumnStatistics> updatedTableColumnStatistics)
    {
        try {
            HiveBasicStatistics tableStats = getHiveBasicStatistics(table.getParameters());
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(table, updatedTableColumnStatistics, tableStats.getRowCount()).stream()
                    .filter(this::isGlueWritable)
                    .collect(toImmutableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

            List<CompletableFuture<Void>> updateFutures = columnChunks.stream().map(columnChunk -> runAsync(
                    () -> {
                        UpdateColumnStatisticsForTableRequest request = (UpdateColumnStatisticsForTableRequest) updateAWSRequest(
                                metastoreContext,
                                new UpdateColumnStatisticsForTableRequest()
                                    .withCatalogId(catalogId)
                                    .withDatabaseName(table.getDatabaseName())
                                    .withTableName(table.getTableName())
                                    .withColumnStatisticsList(columnChunk));
                        glueClient.updateColumnStatisticsForTable(request);
                    }, this.writeExecutor))
                    .collect(toImmutableList());

            Map<String, HiveColumnStatistics> currentTableColumnStatistics = this.getTableColumnStatistics(metastoreContext, table);
            Set<String> removedStatistics = difference(currentTableColumnStatistics.keySet(), updatedTableColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteFutures = removedStatistics.stream()
                    .map(column -> runAsync(
                            () -> {
                                DeleteColumnStatisticsForTableRequest request = (DeleteColumnStatisticsForTableRequest) updateAWSRequest(
                                        metastoreContext,
                                        new DeleteColumnStatisticsForTableRequest()
                                            .withCatalogId(catalogId)
                                            .withDatabaseName(table.getDatabaseName())
                                            .withTableName(table.getTableName())
                                            .withColumnName(column));
                                glueClient.deleteColumnStatisticsForTable(request);
                            }, this.writeExecutor))
                    .collect(toImmutableList());

            ImmutableList<CompletableFuture<Void>> updateOperationsFutures = ImmutableList.<CompletableFuture<Void>>builder()
                    .addAll(updateFutures)
                    .addAll(deleteFutures)
                    .build();

            getFutureValue(CompletableFuture.allOf(updateOperationsFutures.toArray(new CompletableFuture[0])));
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, Partition partition, Map<String, HiveColumnStatistics> updatedColumnStatistics)
    {
        try {
            HiveBasicStatistics partitionStats = getHiveBasicStatistics(partition.getParameters());
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(partition, updatedColumnStatistics, partitionStats.getRowCount()).stream()
                    .filter(this::isGlueWritable)
                    .collect(toImmutableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

            List<CompletableFuture<Void>> writePartitionStatsFutures = columnChunks.stream()
                    .map(columnChunk ->
                            runAsync(() -> {
                                UpdateColumnStatisticsForPartitionRequest request = (UpdateColumnStatisticsForPartitionRequest) updateAWSRequest(
                                        metastoreContext,
                                        new UpdateColumnStatisticsForPartitionRequest()
                                            .withCatalogId(catalogId)
                                            .withDatabaseName(partition.getDatabaseName())
                                            .withTableName(partition.getTableName())
                                            .withPartitionValues(partition.getValues())
                                            .withColumnStatisticsList(columnChunk));
                                glueClient.updateColumnStatisticsForPartition(request);
                            }, writeExecutor))
                    .collect(toImmutableList());

            Map<String, HiveColumnStatistics> currentColumnStatistics = this.getPartitionColumnStatisticsIfPresent(metastoreContext, partition).orElse(ImmutableMap.of());
            Set<String> removedStatistics = difference(currentColumnStatistics.keySet(), updatedColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteStatsFutures = removedStatistics.stream()
                    .map(column -> runAsync(() ->
                    {
                        DeleteColumnStatisticsForPartitionRequest request = (DeleteColumnStatisticsForPartitionRequest) updateAWSRequest(
                                metastoreContext,
                                new DeleteColumnStatisticsForPartitionRequest()
                                    .withCatalogId(catalogId)
                                    .withDatabaseName(partition.getDatabaseName())
                                    .withTableName(partition.getTableName())
                                    .withPartitionValues(partition.getValues())
                                    .withColumnName(column));
                        glueClient.deleteColumnStatisticsForPartition(request);
                    }, writeExecutor))
                    .collect(toImmutableList());

            ImmutableList<CompletableFuture<Void>> updateOperationsFutures = ImmutableList.<CompletableFuture<Void>>builder()
                    .addAll(writePartitionStatsFutures)
                    .addAll(deleteStatsFutures)
                    .build();

            getFutureValue(CompletableFuture.allOf(updateOperationsFutures.toArray(new CompletableFuture[0])));
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private List<String> getAllColumns(Table table)
    {
        ImmutableList.Builder<String> allColumns = ImmutableList.builderWithExpectedSize(table.getDataColumns().size() + table.getPartitionColumns().size());
        table.getDataColumns().stream().map(Column::getName).forEach(allColumns::add);
        table.getPartitionColumns().stream().map(Column::getName).forEach(allColumns::add);
        return allColumns.build();
    }
}
