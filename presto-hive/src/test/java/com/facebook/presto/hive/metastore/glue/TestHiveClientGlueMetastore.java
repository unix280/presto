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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.TableInput;
import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.presto.hive.AbstractTestHiveClientLocal;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.MetastoreContext;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.HiveStorageFormat.CSV;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.DELTA_LAKE_PROVIDER;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.SPARK_TABLE_PROVIDER_KEY;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isDeltaLakeTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isIcebergTable;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Sets.difference;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveClientGlueMetastore
        extends AbstractTestHiveClientLocal
{
    private static final MetastoreContext METASTORE_CONTEXT = new MetastoreContext(SESSION, Optional.empty(), HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);

    public TestHiveClientGlueMetastore()
    {
        super("test_glue" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        this.createTableFormats = difference(
                ImmutableSet.copyOf(HiveStorageFormat.values()),
                // exclude formats that change table schema with serde
                ImmutableSet.of(AVRO, CSV, PAGEFILE));
    }

    /**
     * GlueHiveMetastore currently uses AWS Default Credential Provider Chain,
     * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
     * on ways to set your AWS credentials which will be needed to run this test.
     */
    @Override
    protected ExtendedHiveMetastore createMetastore(File tempDir)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig().setColumnStatisticsEnabled(true);
        glueConfig.setDefaultWarehouseDir(tempDir.toURI().toString());
        GlueSecurityMappingsSupplier glueSecurityMappingsSupplier = new GlueSecurityMappingsSupplier(Optional.empty(), Optional.empty());

        Executor executor = new BoundedExecutor(this.executor, 10);
        return new GlueHiveMetastore(hdfsEnvironment, glueConfig, glueSecurityMappingsSupplier, executor, executor, executor);
    }

    @Override
    public void testRenameTable()
    {
        // rename table is not yet supported by Glue
    }

    @Override
    public void testUpdateTableColumnStatisticsEmptyOptionalFields() throws Exception
    {
        // this test expects consistency between written and read stats but this is not provided by glue at the moment
        // when writing empty min/max statistics glue will return 0 to the readers
        // in order to avoid incorrect data we skip writes for statistics with min/max = null
    }

    @Override
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields() throws Exception
    {
        // this test expects consistency between written and read stats but this is not provided by glue at the moment
        // when writing empty min/max statistics glue will return 0 to the readers
        // in order to avoid incorrect data we skip writes for statistics with min/max = null
    }

    @Override
    public void testTableConstraints()
    {
        // GlueMetastore has no support for table constraints
    }

    @Override
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, BASIC_STATISTICS_1, BASIC_STATISTICS_2, BASIC_STATISTICS_1, EMPTY_TABLE_STATISTICS);
    }

    @Test
    public void testGetPartitions() throws Exception
    {
        try {
            createDummyPartitionedTable(tablePartitionFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
            Optional<Table> table = getMetastoreClient().getTable(METASTORE_CONTEXT, tablePartitionFormat.getSchemaName(), tablePartitionFormat.getTableName());
            Optional<List<String>> partitionNames = getMetastoreClient().getPartitionNames(METASTORE_CONTEXT, table.get());
            assertTrue(partitionNames.isPresent());
            assertEquals(partitionNames.get(), ImmutableList.of("ds=2016-01-01", "ds=2016-01-02"));
        }
        finally {
            dropTable(tablePartitionFormat);
        }
    }

    @Test
    public void testGetDatabasesLogsStats()
    {
        GlueHiveMetastore metastore = (GlueHiveMetastore) getMetastoreClient();
        GlueMetastoreStats stats = metastore.getStats();
        double initialCallCount = stats.getGetDatabases().getTime().getAllTime().getCount();
        long initialFailureCount = stats.getGetDatabases().getTotalFailures().getTotalCount();
        getMetastoreClient().getAllDatabases(METASTORE_CONTEXT);
        assertEquals(stats.getGetDatabases().getTime().getAllTime().getCount(), initialCallCount + 1.0);
        assertTrue(stats.getGetDatabases().getTime().getAllTime().getAvg() > 0.0);
        assertEquals(stats.getGetDatabases().getTotalFailures().getTotalCount(), initialFailureCount);
    }

    @Test
    public void testGetDatabaseFailureLogsStats()
    {
        GlueHiveMetastore metastore = (GlueHiveMetastore) getMetastoreClient();
        GlueMetastoreStats stats = metastore.getStats();
        long initialFailureCount = stats.getGetDatabase().getTotalFailures().getTotalCount();
        assertThrows(() -> getMetastoreClient().getDatabase(METASTORE_CONTEXT, null));
        assertEquals(stats.getGetDatabase().getTotalFailures().getTotalCount(), initialFailureCount + 1);
    }

    public void testTableWithoutStorageDescriptor()
    {
        // StorageDescriptor is an Optional field for Glue tables. Iceberg and Delta Lake tables may not have it set.
        SchemaTableName table = temporaryTable("test_missing_storage_descriptor");
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(table.getSchemaName())
                .withName(table.getTableName());
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        try {
            ConnectorSession session = newSession();
            MetastoreContext metastoreContext = new MetastoreContext(
                    session.getIdentity(),
                    session.getQueryId(),
                    session.getClientInfo(),
                    session.getSource(),
                    getMetastoreHeaders(session),
                    false,
                    DEFAULT_COLUMN_CONVERTER_PROVIDER);
            TableInput tableInput = new TableInput()
                    .withName(table.getTableName())
                    .withTableType(EXTERNAL_TABLE.name());
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));

            assertThatThrownBy(() -> getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()))
                    .hasMessageStartingWith("Table StorageDescriptor is null for table");
            glueClient.deleteTable(deleteTableRequest);

            // Iceberg table
            tableInput = tableInput.withParameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE));
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));
            assertTrue(isIcebergTable(getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()).orElseThrow(() -> new NoSuchElementException())));
            glueClient.deleteTable(deleteTableRequest);

            // Delta Lake table
            tableInput = tableInput.withParameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER));
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));
            assertTrue(isDeltaLakeTable(getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()).orElseThrow(() -> new NoSuchElementException())));
        }
        finally {
            // Table cannot be dropped through HiveMetastore since a TableHandle cannot be created
            glueClient.deleteTable(new DeleteTableRequest()
                    .withDatabaseName(table.getSchemaName())
                    .withName(table.getTableName()));
        }
    }
}
