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
package com.facebook.presto.hive.security.lakeformation;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetUnfilteredTableMetadataRequest;
import com.amazonaws.services.glue.model.GetUnfilteredTableMetadataResult;
import com.amazonaws.services.securitytoken.model.Tag;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.authentication.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreConfig;
import com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.glue.GlueMetastoreStats;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMapping;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappings;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingsSupplier;
import com.facebook.presto.hive.security.lakeformation.annotations.ForLakeFormationSecurity;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jetbrains.annotations.TestOnly;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastore.AHANA;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastore.LAKE_FORMATION_AUTHORIZED_CALLER;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LakeFormationAccessControl
        implements ConnectorAccessControl
{
    private static final Logger log = Logger.get(LakeFormationAccessControl.class);

    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final GlueMetastoreStats stats = new GlueMetastoreStats();
    private AWSGlueAsync glueClient;
    private String catalogId;
    private String catalogIamRole;
    private String lakeFormationPartnerTagName;
    private String lakeFormationPartnerTagValue;
    private Optional<String> supportedPermissionType;
    private boolean impersonationEnabled;
    private Supplier<GlueSecurityMappings> mappings;
    private LoadingCache<String, AWSCredentialsProvider> awsCredentialsProviderLoadingCache;
    private LoadingCache<LFPolicyCacheKey, Optional<GetUnfilteredTableMetadataResult>> lakeFormationPolicyCache;

    @TestOnly
    public LakeFormationAccessControl()
    {
    }

    @TestOnly
    public void setCatalogIamRole(String catalogIamRole)
    {
        this.catalogIamRole = catalogIamRole;
    }

    @TestOnly
    public void setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
    }

    @TestOnly
    public void setMappings(Supplier<GlueSecurityMappings> mappings)
    {
        this.mappings = mappings;
    }

    @TestOnly
    public void setLakeFormationPolicyCache(LoadingCache<LFPolicyCacheKey, Optional<GetUnfilteredTableMetadataResult>> lakeFormationPolicyCache)
    {
        this.lakeFormationPolicyCache = lakeFormationPolicyCache;
    }

    @Inject
    public LakeFormationAccessControl(
            LakeFormationSecurityConfig lakeFormationSecurityConfig,
            GlueHiveMetastoreConfig glueHiveMetastoreConfig,
            @ForLakeFormationSecurity GlueSecurityMappingsSupplier glueSecurityMappingsSupplier,
            MetastoreConfig metastoreConfig,
            @ForLakeFormationSecurity Executor lfPolicyRefreshExecutor)
    {
        verify(metastoreConfig.getMetastoreType().equals("glue"), "Lake Formation Security can only be configured for Glue Metastore");

        this.glueClient = createAsyncGlueClient(requireNonNull(glueHiveMetastoreConfig, "glueConfig is null"), stats.newRequestMetricsCollector());
        this.catalogId = glueHiveMetastoreConfig.getCatalogId().orElse(null);
        this.catalogIamRole = glueHiveMetastoreConfig.getIamRole().orElse("defaultIAMRole");
        this.lakeFormationPartnerTagName = glueHiveMetastoreConfig.getLakeFormationPartnerTagName().orElse(LAKE_FORMATION_AUTHORIZED_CALLER);
        this.lakeFormationPartnerTagValue = glueHiveMetastoreConfig.getLakeFormationPartnerTagValue().orElse(AHANA);
        this.supportedPermissionType = glueHiveMetastoreConfig.getSupportedPermissionType();
        this.impersonationEnabled = glueHiveMetastoreConfig.isImpersonationEnabled();
        this.mappings = requireNonNull(glueSecurityMappingsSupplier, "glueSecurityMappingsSupplier is null").getMappingsSupplier();

        verify(!impersonationEnabled || mappings != null, "Glue Security Mapping has to be configured if impersonation is enabled");
        verify(mappings == null || impersonationEnabled, "Impersonation has to be enabled to use Glue Security Mapping");

        this.awsCredentialsProviderLoadingCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(this::createAssumeRoleCredentialsProvider));

        this.lakeFormationPolicyCache = CacheBuilder.newBuilder()
                .expireAfterWrite(lakeFormationSecurityConfig.getCacheTTL().toMillis(), MILLISECONDS)
                .refreshAfterWrite(lakeFormationSecurityConfig.getRefreshPeriod().toMillis(), MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(this::getUnfilteredTableResult), lfPolicyRefreshExecutor));
    }

    private static AWSGlueAsync createAsyncGlueClient(GlueHiveMetastoreConfig config, RequestMetricCollector metricsCollector)
    {
        ClientConfiguration clientConfig = new ClientConfiguration().withMaxConnections(config.getMaxGlueConnections());
        AWSGlueAsyncClientBuilder asyncGlueClientBuilder = AWSGlueAsyncClientBuilder.standard()
                .withMetricsCollector(metricsCollector)
                .withClientConfiguration(clientConfig);

        if (config.getGlueRegion().isPresent()) {
            asyncGlueClientBuilder.setRegion(config.getGlueRegion().get());
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            Region currentRegion = Regions.getCurrentRegion();
            if (currentRegion != null) {
                asyncGlueClientBuilder.setRegion(currentRegion.getName());
            }
        }

        if (config.getIamRole().isPresent()) {
            Collection<Tag> tags = new ArrayList<>();
            Tag tag = new Tag()
                    .withKey(config.getLakeFormationPartnerTagName().orElse(LAKE_FORMATION_AUTHORIZED_CALLER))
                    .withValue(config.getLakeFormationPartnerTagValue().orElse(AHANA));
            tags.add(tag);

            AWSCredentialsProvider credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
                    .Builder(config.getIamRole().get(), "roleSessionName")
                    .withSessionTags(tags)
                    .build();
            asyncGlueClientBuilder.setCredentials(credentialsProvider);
        }

        return asyncGlueClientBuilder.build();
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    /**
     * Check if identity is allowed to create the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to drop the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to rename the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName, String newSchemaName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must handle filter all results for unauthorized users,
     * since there are multiple way to list schemas.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context)
    {
        // allow
    }

    /**
     * Filter the list of schemas to those visible to the identity.
     */
    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<String> schemaNames)
    {
        return schemaNames;
    }

    /**
     * Check if identity is allowed to create the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to drop the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to rename the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        // allow
    }

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    /**
     * Check if identity is allowed to add columns to the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to drop columns from the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to select from the specified columns in a relation.  The column set can be empty.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return;
        }

        LFPolicyCacheKey lfPolicyCacheKey = getLfPolicyCacheKey(identity, tableName);

        Optional<GetUnfilteredTableMetadataResult> result = lakeFormationPolicyCache.getUnchecked(lfPolicyCacheKey);
        if (!result.isPresent()) {
            denySelectTable(tableName.getTableName(), format("Access Denied: " +
                    "Error fetching table [%s/%s] or table does not exist", tableName.getSchemaName(), tableName.getTableName()));
        }
        if (result.get().isRegisteredWithLakeFormation()) {
            List<String> authorizedColumns = result.get().getAuthorizedColumns();
            Set<String> deniedColumns = new HashSet<>();
            columnNames.stream()
                    .filter(columnName -> !authorizedColumns.contains(columnName))
                    .forEach(deniedColumns::add);

            if (!deniedColumns.isEmpty()) {
                denySelectTable(tableName.getTableName(), format("Access Denied: User [ %s ] does not have [SELECT] " +
                        "privilege on [ %s ] columns of [ %s/%s ]", identity.getUser(), deniedColumns, tableName.getSchemaName(), tableName.getTableName()));
            }
        }
    }

    /**
     * Check if identity is allowed to insert into the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to delete from the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to create the specified view in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to drop the specified view in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        // allow
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified columns in a relation.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return;
        }

        LFPolicyCacheKey lfPolicyCacheKey = getLfPolicyCacheKey(identity, tableName);

        Optional<GetUnfilteredTableMetadataResult> result = lakeFormationPolicyCache.getUnchecked(lfPolicyCacheKey);
        if (!result.isPresent()) {
            denyCreateViewWithSelect(tableName.getTableName(), identity, format("Access Denied: " +
                    "Error fetching table [%s/%s] or table does not exist", tableName.getSchemaName(), tableName.getTableName()));
        }
        if (result.get().isRegisteredWithLakeFormation()) {
            List<String> authorizedColumns = result.get().getAuthorizedColumns();
            Set<String> deniedColumns = new HashSet<>();
            columnNames.stream()
                    .filter(columnName -> !authorizedColumns.contains(columnName))
                    .forEach(deniedColumns::add);

            if (!deniedColumns.isEmpty()) {
                denyCreateViewWithSelect(tableName.getTableName(), identity, format("Access Denied: User [ %s ] does not have [SELECT] " +
                        "privilege on [ %s ] columns of [ %s/%s ]", identity.getUser(), deniedColumns, tableName.getSchemaName(), tableName.getTableName()));
            }
        }
    }

    /**
     * Check if identity is allowed to set the specified property in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String propertyName)
    {
        // allow
    }

    private AWSCredentialsProvider createAssumeRoleCredentialsProvider(String iamRole)
    {
        Collection<Tag> tags = new ArrayList<>();
        Tag tag = new Tag().withKey(lakeFormationPartnerTagName).withValue(lakeFormationPartnerTagValue);
        tags.add(tag);

        return new STSAssumeRoleSessionCredentialsProvider
                .Builder(iamRole, "roleSessionName")
                .withSessionTags(tags)
                .build();
    }

    private String getGlueIamRole(MetastoreContext metastoreContext)
    {
        GlueSecurityMapping mapping = mappings.get().getMapping(metastoreContext)
                .orElseThrow(() -> new AccessDeniedException("No matching Glue Security Mapping or Glue Security Mapping has no role"));

        return mapping.getIamRole();
    }

    private Optional<GetUnfilteredTableMetadataResult> getUnfilteredTableResult(LFPolicyCacheKey lfPolicyCacheKey)
    {
        GetUnfilteredTableMetadataRequest request;
        List<String> supportedPermissionTypes = new ArrayList<>();
        supportedPermissionType.ifPresent(supportedPermissionTypes::add);

        if (impersonationEnabled) {
            request = new GetUnfilteredTableMetadataRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(lfPolicyCacheKey.getSchemaTableName().getSchemaName())
                    .withName(lfPolicyCacheKey.getSchemaTableName().getTableName())
                    .withSupportedPermissionTypes(supportedPermissionTypes)
                    .withRequestCredentialsProvider(awsCredentialsProviderLoadingCache.getUnchecked(lfPolicyCacheKey.getIamRole()));
        }
        else {
            request = new GetUnfilteredTableMetadataRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(lfPolicyCacheKey.getSchemaTableName().getSchemaName())
                    .withName(lfPolicyCacheKey.getSchemaTableName().getTableName())
                    .withSupportedPermissionTypes(supportedPermissionTypes);
        }

        try {
            return Optional.of(glueClient.getUnfilteredTableMetadata(request));
        }
        catch (EntityNotFoundException e) {
            log.error(e, "Table not found");
            return Optional.empty();
        }
        catch (AmazonServiceException e) {
            log.error(e, "Error while calling getUnfilteredTable API");
            return Optional.empty();
        }
    }

    private LFPolicyCacheKey getLfPolicyCacheKey(ConnectorIdentity identity, SchemaTableName tableName)
    {
        LFPolicyCacheKey lfPolicyCacheKey;

        if (impersonationEnabled) {
            String iamRole = getGlueIamRole(new MetastoreContext(identity));
            lfPolicyCacheKey = new LFPolicyCacheKey(tableName, iamRole);
        }
        else {
            lfPolicyCacheKey = new LFPolicyCacheKey(tableName, catalogIamRole);
        }
        return lfPolicyCacheKey;
    }

    protected static final class LFPolicyCacheKey
    {
        private final SchemaTableName schemaTableName;
        private final String iamRole;

        LFPolicyCacheKey(SchemaTableName schemaTableName, String iamRole)
        {
            this.schemaTableName = schemaTableName;
            this.iamRole = iamRole;
        }

        private SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        private String getIamRole()
        {
            return iamRole;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schemaTableName, iamRole);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            LFPolicyCacheKey other = (LFPolicyCacheKey) obj;
            return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                    Objects.equals(this.iamRole, other.iamRole);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("schemaTableName", schemaTableName)
                    .add("iamRole", iamRole)
                    .toString();
        }
    }
}
