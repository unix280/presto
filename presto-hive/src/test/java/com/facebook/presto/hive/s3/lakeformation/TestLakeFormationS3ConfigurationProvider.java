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
package com.facebook.presto.hive.s3.lakeformation;

import com.amazonaws.services.lakeformation.model.GetTemporaryTableCredentialsResult;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingsSupplier;
import com.facebook.presto.hive.s3.lakeformation.LakeFormationS3ConfigurationProvider.TableCredentialsCacheKey;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SESSION_TOKEN;
import static com.facebook.presto.hive.s3.lakeformation.TestLakeFormationS3ConfigurationProvider.MappingSelector.empty;
import static com.google.common.io.Resources.getResource;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestLakeFormationS3ConfigurationProvider
{
    private static final String DEFAULT_USER = "defaultUser";
    private static final String TABLE_ARN = "arn:aws:glue:us-east-1:789986721738:table/testSchema/testTable";
    private static final String ADMIN_IAM_ROLE = "arn:aws:iam::789986721738:role/test_admin_role";
    private static final String ANALYST_IAM_ROLE = "arn:aws:iam::789986721738:role/test_analyst_role";
    private static final String DEFAULT_IAM_ROLE = "arn:aws:iam::789986721738:role/test_default_role";

    private final Map<TableCredentialsCacheKey, Optional<GetTemporaryTableCredentialsResult>> mockGetTemporaryTableCredentialsResult = new HashMap<>();
    private DynamicConfigurationProvider provider;

    /*
    We will be creating a mock Map which will be containing sample
    GetTemporaryTableCredentialsResult objects for different TableCredentialsCacheKey keys.
    This map will act as a substitute for AWS API calls for fetching
    temporary table credentials.
     */
    @BeforeClass
    public void setUp()
    {
        this.provider = createLakeFormationS3ConfigurationProvider();

        // User: admin
        TableCredentialsCacheKey adminTableCredentialsCacheKey = new TableCredentialsCacheKey(TABLE_ARN, ADMIN_IAM_ROLE);
        GetTemporaryTableCredentialsResult adminGetTemporaryTableCredentialsResult = new GetTemporaryTableCredentialsResult()
                .withAccessKeyId("adminAccessKey")
                .withSecretAccessKey("adminSecretKey")
                .withSessionToken("adminSessionToken");

        mockGetTemporaryTableCredentialsResult.put(adminTableCredentialsCacheKey, Optional.of(adminGetTemporaryTableCredentialsResult));

        // User: analyst
        TableCredentialsCacheKey analystTableCredentialsCacheKey = new TableCredentialsCacheKey(TABLE_ARN, ANALYST_IAM_ROLE);
        GetTemporaryTableCredentialsResult analystGetTemporaryTableCredentialsResult = new GetTemporaryTableCredentialsResult()
                .withAccessKeyId("analystAccessKey")
                .withSecretAccessKey("analystSecretKey")
                .withSessionToken("analystSessionToken");

        mockGetTemporaryTableCredentialsResult.put(analystTableCredentialsCacheKey, Optional.of(analystGetTemporaryTableCredentialsResult));

        // User: defaultUser
        TableCredentialsCacheKey defaultTableCredentialsCacheKey = new TableCredentialsCacheKey(TABLE_ARN, DEFAULT_IAM_ROLE);
        mockGetTemporaryTableCredentialsResult.put(defaultTableCredentialsCacheKey, Optional.empty());
    }

    @Test
    public void testCredentialVending()
    {
        // match user - admin and successfully vend credentials
        assertMapping(
                provider,
                empty().withUser("admin"),
                MappingResult.credentials("adminAccessKey", "adminSecretKey", "adminSessionToken"));

        // match user - analyst and successfully vend credentials
        assertMapping(
                provider,
                empty().withUser("analyst"),
                MappingResult.credentials("analystAccessKey", "analystSecretKey", "analystSessionToken"));

        // assert exception while vending credentials
        assertMappingFails(
                provider,
                empty().withUser(DEFAULT_USER),
                String.format("Error vending credentials or table [%s] does not exist", TABLE_ARN));
    }

    private static void assertMapping(DynamicConfigurationProvider provider, MappingSelector selector, MappingResult mappingResult)
    {
        Configuration configuration = new Configuration(false);

        assertNull(configuration.get(S3_ACCESS_KEY));
        assertNull(configuration.get(S3_SECRET_KEY));
        assertNull(configuration.get(S3_SESSION_TOKEN));

        applyMapping(provider, selector, configuration);

        assertEquals(configuration.get(S3_ACCESS_KEY), mappingResult.getAccessKey());
        assertEquals(configuration.get(S3_SECRET_KEY), mappingResult.getSecretKey());
        assertEquals(configuration.get(S3_SESSION_TOKEN), mappingResult.getSessionToken());
    }

    private static void assertMappingFails(DynamicConfigurationProvider provider, MappingSelector selector, String message)
    {
        Configuration configuration = new Configuration(false);

        assertThatThrownBy(() -> applyMapping(provider, selector, configuration))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: " + message);
    }

    private static void applyMapping(DynamicConfigurationProvider provider, MappingSelector selector, Configuration configuration)
    {
        provider.updateConfiguration(configuration, selector.getHdfsContext(), new Path("path").toUri());
    }

    private DynamicConfigurationProvider createLakeFormationS3ConfigurationProvider()
    {
        GlueHiveMetastoreConfig glueHiveMetastoreConfig = new GlueHiveMetastoreConfig()
                .setImpersonationEnabled(true)
                .setGlueRegion("us-east-1")
                .setCatalogId("789986721738");

        GlueSecurityMappingConfig glueSecurityMappingConfig = new GlueSecurityMappingConfig()
                .setConfigFile(new File(getResource("com.facebook.presto.hive.metastore.glue/glue-security-mapping.json").getPath()));
        GlueSecurityMappingsSupplier glueSecurityMappingsSupplier = new GlueSecurityMappingsSupplier(glueSecurityMappingConfig.getConfigFile(), Optional.empty());

        return new LakeFormationS3ConfigurationProvider(glueHiveMetastoreConfig, glueSecurityMappingsSupplier)
                .setTableCredentialsLoadingCache(CacheBuilder.newBuilder().build(CacheLoader.from(this::mockGetTemporaryTableCredentials)));
    }

    private Optional<GetTemporaryTableCredentialsResult> mockGetTemporaryTableCredentials(TableCredentialsCacheKey tableCredentialsCacheKey)
    {
        if (mockGetTemporaryTableCredentialsResult.containsKey(tableCredentialsCacheKey)) {
            return mockGetTemporaryTableCredentialsResult.get(tableCredentialsCacheKey);
        }

        return Optional.empty();
    }

    public static class MappingSelector
    {
        public static MappingSelector empty()
        {
            return new MappingSelector(DEFAULT_USER);
        }

        private final String user;

        private MappingSelector(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user);
        }

        public HdfsContext getHdfsContext()
        {
            ConnectorSession connectorSession = new TestingConnectorSession(
                    new ConnectorIdentity(
                            user, Optional.empty(), Optional.empty(), emptyMap(), emptyMap()),
                    new HiveSessionProperties(
                            new HiveClientConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()
                    ).getSessionProperties());

            return new HdfsContext(connectorSession, "testSchema", "testTable", "path", false);
        }
    }

    public static class MappingResult
    {
        public static MappingResult credentials(String accessKey, String secretKey, String sessionToken)
        {
            return new MappingResult(accessKey, secretKey, sessionToken);
        }

        private final String accessKey;
        private final String secretKey;
        private final String sessionToken;

        private MappingResult(String accessKey, String secretKey, String sessionToken)
        {
            this.accessKey = requireNonNull(accessKey, "accessKey is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.sessionToken = requireNonNull(sessionToken, "sessionToken is null");
        }

        public String getAccessKey()
        {
            return accessKey;
        }

        public String getSecretKey()
        {
            return secretKey;
        }

        public String getSessionToken()
        {
            return sessionToken;
        }
    }
}
