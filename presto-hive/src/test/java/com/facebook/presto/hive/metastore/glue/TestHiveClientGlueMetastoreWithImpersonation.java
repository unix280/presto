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
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.HiveStorageFormat.CSV;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.google.common.collect.Sets.difference;
import static com.google.common.io.Resources.getResource;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveClientGlueMetastoreWithImpersonation
        extends AbstractTestHiveClientLocal
{
    public static final String ADMIN = "admin";
    public static final String ANALYST = "analyst";

    public TestHiveClientGlueMetastoreWithImpersonation()
    {
        super("test_glue_impersonation" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        this.createTableFormats = difference(
                ImmutableSet.copyOf(HiveStorageFormat.values()),
                // exclude formats that change table schema with serde
                ImmutableSet.of(AVRO, CSV, PAGEFILE));
    }

    @Override
    protected ExtendedHiveMetastore createMetastore(File tempDir)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig().setColumnStatisticsEnabled(true);
        glueConfig.setDefaultWarehouseDir(tempDir.toURI().toString());
        glueConfig.setImpersonationEnabled(true);
        GlueSecurityMappingConfig glueSecurityMappingConfig = new GlueSecurityMappingConfig()
                .setConfigFile(new File(getResource("com.facebook.presto.hive.metastore.glue/glue-security-mapping.json").getPath()));
        GlueSecurityMappingsSupplier glueSecurityMappingsSupplier = new GlueSecurityMappingsSupplier(glueSecurityMappingConfig.getConfigFile(), glueSecurityMappingConfig.getRefreshPeriod());

        Executor executor = new BoundedExecutor(this.executor, 10);
        return new GlueHiveMetastore(hdfsEnvironment, glueConfig, glueSecurityMappingsSupplier, executor, executor, executor);
    }

    @Override
    public void testRenameTable()
    {
        // rename table is not yet supported by Glue
    }

    @Override
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
    {
        // this test expects consistency between written and read stats but this is not provided by glue at the moment
        // when writing empty min/max statistics glue will return 0 to the readers
        // in order to avoid incorrect data we skip writes for statistics with min/max = null
    }

    @Override
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
    {
        // this test expects consistency between written and read stats but this is not provided by glue at the moment
        // when writing empty min/max statistics glue will return 0 to the readers
        // in order to avoid incorrect data we skip writes for statistics with min/max = null
    }

    @Override
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, BASIC_STATISTICS_1, BASIC_STATISTICS_2, BASIC_STATISTICS_1, EMPTY_TABLE_STATISTICS);
    }

    @Override
    public void testGetPartitions() throws Exception
    {
        try {
            createDummyPartitionedTable(tablePartitionFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
            Optional<List<String>> partitionNames = getMetastoreClient().getPartitionNames(getMetastoreContext(ADMIN), tablePartitionFormat.getSchemaName(), tablePartitionFormat.getTableName());
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
        getMetastoreClient().getAllDatabases(getMetastoreContext(ANALYST));
        assertEquals(stats.getGetDatabases().getTime().getAllTime().getCount(), initialCallCount + 1.0);
        assertTrue(stats.getGetDatabases().getTime().getAllTime().getAvg() > 0.0);
        assertEquals(stats.getGetDatabases().getTotalFailures().getTotalCount(), initialFailureCount);
    }

    @Override
    public void testTableConstraints()
    {
        // GlueMetastore has no support for table constraints
    }

    @Test
    public void testGetDatabaseFailureLogsStats()
    {
        GlueHiveMetastore metastore = (GlueHiveMetastore) getMetastoreClient();
        GlueMetastoreStats stats = metastore.getStats();
        long initialFailureCount = stats.getGetDatabase().getTotalFailures().getTotalCount();
        assertThrows(() -> getMetastoreClient().getDatabase(getMetastoreContext(ADMIN), null));
        assertEquals(stats.getGetDatabase().getTotalFailures().getTotalCount(), initialFailureCount + 1);
    }

    private MetastoreContext getMetastoreContext(String user)
    {
        ConnectorIdentity connectorIdentity = new ConnectorIdentity(
                user, Optional.empty(), Optional.empty(), emptyMap(), emptyMap());

        String queryId = "test_queryId";

        return new MetastoreContext(connectorIdentity, queryId, Optional.empty(), HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
    }
}
