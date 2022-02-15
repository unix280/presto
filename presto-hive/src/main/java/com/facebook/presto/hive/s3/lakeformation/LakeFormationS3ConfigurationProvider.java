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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.lakeformation.AWSLakeFormationAsync;
import com.amazonaws.services.lakeformation.AWSLakeFormationAsyncClientBuilder;
import com.amazonaws.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import com.amazonaws.services.lakeformation.model.GetTemporaryGlueTableCredentialsResult;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsync;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.Tag;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.authentication.MetastoreContext;
import com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMapping;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappings;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingsSupplier;
import com.facebook.presto.hive.s3.lakeformation.annotations.ForLFCredentialVending;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.TestOnly;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastore.AHANA;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastore.LAKE_FORMATION_AUTHORIZED_CALLER;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_LAKE_FORMATION_CACHE_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SESSION_TOKEN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LakeFormationS3ConfigurationProvider
        implements DynamicConfigurationProvider
{
    private static final Logger log = Logger.get(LakeFormationS3ConfigurationProvider.class);

    private final String glueRegion;
    private final String catalogId;
    private final String catalogIamRole;
    private final String lakeFormationPartnerTagName;
    private final String lakeFormationPartnerTagValue;
    private final Optional<String> supportedPermissionType;
    private final boolean impersonationEnabled;
    private final Supplier<GlueSecurityMappings> mappings;
    private final LoadingCache<String, AWSCredentialsProvider> awsCredentialsProviderLoadingCache;
    private final LoadingCache<String, Optional<String>> accountIdLoadingCache;
    private LoadingCache<TableCredentialsCacheKey, Optional<GetTemporaryGlueTableCredentialsResult>> tableCredentialsLoadingCache;
    private final AWSLakeFormationAsync lakeFormationClient;
    private final AWSSecurityTokenServiceAsync stsClient;

    @TestOnly
    public LakeFormationS3ConfigurationProvider setTableCredentialsLoadingCache(LoadingCache<TableCredentialsCacheKey, Optional<GetTemporaryGlueTableCredentialsResult>> tableCredentialsLoadingCache)
    {
        this.tableCredentialsLoadingCache = tableCredentialsLoadingCache;
        return this;
    }

    @Inject
    public LakeFormationS3ConfigurationProvider(
            GlueHiveMetastoreConfig glueHiveMetastoreConfig,
            @ForLFCredentialVending GlueSecurityMappingsSupplier glueSecurityMappingsSupplier)
    {
        this(
                requireNonNull(glueSecurityMappingsSupplier, "glueSecurityMappingsSupplier is null").getMappingsSupplier(),
                glueHiveMetastoreConfig);
    }

    private LakeFormationS3ConfigurationProvider(Supplier<GlueSecurityMappings> mappings, GlueHiveMetastoreConfig glueHiveMetastoreConfig)
    {
        this.mappings = mappings;
        this.lakeFormationPartnerTagName = glueHiveMetastoreConfig.getLakeFormationPartnerTagName().orElse(LAKE_FORMATION_AUTHORIZED_CALLER);
        this.lakeFormationPartnerTagValue = glueHiveMetastoreConfig.getLakeFormationPartnerTagValue().orElse(AHANA);
        this.supportedPermissionType = glueHiveMetastoreConfig.getSupportedPermissionType();
        this.impersonationEnabled = glueHiveMetastoreConfig.isImpersonationEnabled();

        verify(!impersonationEnabled || mappings != null, "Glue Security Mapping has to be configured if impersonation is enabled");
        verify(mappings == null || impersonationEnabled, "Impersonation has to be enabled to use Glue Security Mapping");

        this.awsCredentialsProviderLoadingCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(this::createAssumeRoleCredentialsProvider));
        this.accountIdLoadingCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(this::getAccountId));

        // Cache expiration time set to 3000 seconds as default credential expiration time from LF APIs is 3600 seconds
        // This provides enough buffer for the credentials to get refreshed in time
        this.tableCredentialsLoadingCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(3000, SECONDS)
                .build(CacheLoader.from(this::getTemporaryTableCredentials));
        this.glueRegion = getGlueRegion(glueHiveMetastoreConfig);
        this.lakeFormationClient = createAsyncLakeFormationClient(requireNonNull(glueHiveMetastoreConfig, "glueHiveMetastoreConfig is null"), glueRegion);
        this.stsClient = createAsyncSecurityTokenServiceClient(requireNonNull(glueHiveMetastoreConfig, "glueHiveMetastoreConfig is null"), glueRegion);
        this.catalogId = glueHiveMetastoreConfig.getCatalogId().orElse(null);
        this.catalogIamRole = glueHiveMetastoreConfig.getIamRole().orElse(null);

        verify(mappings != null || catalogIamRole != null, "One of Glue Security Mapping or Glue IAM Role needs to be configured to use Lake Formation Security");
    }

    private static String getGlueRegion(GlueHiveMetastoreConfig glueHiveMetastoreConfig)
    {
        if (glueHiveMetastoreConfig.getGlueRegion().isPresent()) {
            return glueHiveMetastoreConfig.getGlueRegion().get();
        }
        else if (glueHiveMetastoreConfig.getPinGlueClientToCurrentRegion()) {
            Region currentRegion = Regions.getCurrentRegion();
            if (currentRegion != null) {
                return currentRegion.getName();
            }
        }
        return null;
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        String iamRole = catalogIamRole;
        if (impersonationEnabled) {
            MetastoreContext metastoreContext = new MetastoreContext(context.getIdentity());
            iamRole = getGlueIamRole(metastoreContext);
        }
        String region;
        try {
            region = glueRegion != null ? glueRegion : new DefaultAwsRegionProviderChain().getRegion();
        }
        catch (SdkClientException e) {
            throw new AccessDeniedException(
                    "Unable to find a region via the region provider chain. " +
                    "Must provide an explicit region in the builder or setup environment to supply a region.");
        }

        String accountId = catalogId != null ? catalogId : accountIdLoadingCache.getUnchecked(iamRole).orElse(null);
        String tableArn = formTableArn(region, accountId, context.getSchemaName().orElse(null), context.getTableName().orElse(null));

        if (tableArn != null) {
            TableCredentialsCacheKey tableCredentialsCacheKey = new TableCredentialsCacheKey(tableArn, iamRole);

            GetTemporaryGlueTableCredentialsResult result;
            result = tableCredentialsLoadingCache.getUnchecked(tableCredentialsCacheKey).orElse(null);

            if (result != null && !(isNullOrEmpty(result.getAccessKeyId()) || isNullOrEmpty(result.getSecretAccessKey()) || isNullOrEmpty(result.getSessionToken()))) {
                configuration.set(S3_ACCESS_KEY, result.getAccessKeyId());
                configuration.set(S3_SECRET_KEY, result.getSecretAccessKey());
                configuration.set(S3_SESSION_TOKEN, result.getSessionToken());

                // Set a cache key to differentiate between filesystem objects in PrestoFileSystemCache for the same S3 authority.
                configuration.set(S3_LAKE_FORMATION_CACHE_KEY, tableCredentialsCacheKey.toString());
            }
        }
    }

    private Optional<GetTemporaryGlueTableCredentialsResult> getTemporaryTableCredentials(TableCredentialsCacheKey tableCredentialsCacheKey)
    {
        try {
            List<String> supportedPermissionTypes = new ArrayList<>();
            supportedPermissionType.ifPresent(supportedPermissionTypes::add);

            if (impersonationEnabled) {
                AWSCredentialsProvider credentialsProvider = awsCredentialsProviderLoadingCache.getUnchecked(tableCredentialsCacheKey.getIamRole());

                return Optional.of(lakeFormationClient.getTemporaryGlueTableCredentials(new GetTemporaryGlueTableCredentialsRequest()
                        .withTableArn(tableCredentialsCacheKey.getTableArn())
                        .withSupportedPermissionTypes(supportedPermissionTypes)
                        .withRequestCredentialsProvider(credentialsProvider)));
            }
            else {
                return Optional.of(lakeFormationClient.getTemporaryGlueTableCredentials(new GetTemporaryGlueTableCredentialsRequest()
                        .withTableArn(tableCredentialsCacheKey.getTableArn())
                        .withSupportedPermissionTypes(supportedPermissionTypes)));
            }
        }
        catch (EntityNotFoundException e) {
            log.error(e, "Table not found");
            return Optional.empty();
        }
        catch (AmazonServiceException e) {
            log.error(e, "Error while calling getTemporaryTableCredentials API");
            return Optional.empty();
        }
    }

    private Optional<String> getAccountId(String iamRole)
    {
        try {
            if (impersonationEnabled) {
                AWSCredentialsProvider awsCredentialsProvider = awsCredentialsProviderLoadingCache.getUnchecked(iamRole);

                return Optional.of(stsClient.getCallerIdentity(new GetCallerIdentityRequest()
                        .withRequestCredentialsProvider(awsCredentialsProvider)).getAccount());
            }
            else {
                return Optional.of(stsClient.getCallerIdentity(new GetCallerIdentityRequest()).getAccount());
            }
        }
        catch (AmazonServiceException e) {
            log.error(e, "Error while calling getCallerIdentity API");
            return Optional.empty();
        }
    }

    private String formTableArn(String region, String accountId, String databaseName, String tableName)
    {
        String tableArn = "arn:aws:glue:%s:%s:table/%s/%s";

        if (databaseName != null && tableName != null && accountId != null) {
            return String.format(tableArn, region, accountId, databaseName, tableName);
        }
        return null;
    }

    private static AWSLakeFormationAsync createAsyncLakeFormationClient(GlueHiveMetastoreConfig glueHiveMetastoreConfig, String glueRegion)
    {
        ClientConfiguration clientConfig = new ClientConfiguration().withMaxConnections(glueHiveMetastoreConfig.getMaxGlueConnections());
        AWSLakeFormationAsyncClientBuilder awsLakeFormationAsyncClientBuilder = AWSLakeFormationAsyncClientBuilder.standard()
                .withClientConfiguration(clientConfig);

        if (glueRegion != null) {
            awsLakeFormationAsyncClientBuilder.setRegion(glueRegion);
        }

        if (glueHiveMetastoreConfig.getIamRole().isPresent()) {
            Collection<Tag> tags = new ArrayList<>();
            Tag tag = new Tag()
                    .withKey(glueHiveMetastoreConfig.getLakeFormationPartnerTagName().orElse(LAKE_FORMATION_AUTHORIZED_CALLER))
                    .withValue(glueHiveMetastoreConfig.getLakeFormationPartnerTagValue().orElse(AHANA));
            tags.add(tag);

            AWSCredentialsProvider credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
                    .Builder(glueHiveMetastoreConfig.getIamRole().get(), "roleSessionName")
                    .withSessionTags(tags)
                    .build();

            awsLakeFormationAsyncClientBuilder.setCredentials(credentialsProvider);
        }

        return awsLakeFormationAsyncClientBuilder.build();
    }

    private static AWSSecurityTokenServiceAsync createAsyncSecurityTokenServiceClient(GlueHiveMetastoreConfig glueHiveMetastoreConfig, String region)
    {
        AWSSecurityTokenServiceAsyncClientBuilder awsSecurityTokenServiceAsyncClientBuilder = AWSSecurityTokenServiceAsyncClientBuilder.standard();

        if (region != null) {
            awsSecurityTokenServiceAsyncClientBuilder.setRegion(region);
        }

        if (glueHiveMetastoreConfig.getIamRole().isPresent()) {
            AWSCredentialsProvider credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
                    .Builder(glueHiveMetastoreConfig.getIamRole().get(), "roleSessionName")
                    .build();

            awsSecurityTokenServiceAsyncClientBuilder.setCredentials(credentialsProvider);
        }

        return awsSecurityTokenServiceAsyncClientBuilder.build();
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

    protected static final class TableCredentialsCacheKey
    {
        private final String tableArn;
        private final String iamRole;

        TableCredentialsCacheKey(String tableArn, String iamRole)
        {
            this.tableArn = tableArn;
            this.iamRole = iamRole;
        }

        private String getTableArn()
        {
            return tableArn;
        }

        private String getIamRole()
        {
            return iamRole;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableArn, iamRole);
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
            TableCredentialsCacheKey other = (TableCredentialsCacheKey) obj;
            return Objects.equals(this.tableArn, other.tableArn) &&
                    Objects.equals(this.iamRole, other.iamRole);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("tableArn", tableArn)
                    .add("iamRole", iamRole)
                    .toString();
        }
    }
}
