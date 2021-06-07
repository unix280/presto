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
package com.facebook.presto.hive.s3;

import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.facebook.presto.hive.s3.TestS3SecurityMapping.MappingResult.credentials;
import static com.facebook.presto.hive.s3.TestS3SecurityMapping.MappingResult.role;
import static com.facebook.presto.hive.s3.TestS3SecurityMapping.MappingSelector.empty;
import static com.facebook.presto.hive.s3.TestS3SecurityMapping.MappingSelector.path;
import static com.google.common.io.Resources.getResource;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestS3SecurityMapping
{
    private static final String IAM_ROLE_CREDENTIAL_NAME = "IAM_ROLE_CREDENTIAL_NAME";
    private static final String DEFAULT_PATH = "s3://default";
    private static final String DEFAULT_USER = "testuser";

    @Test
    public void testMapping()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFile(new File(getResource(getClass(), "security-mapping.json").getPath()))
                .setRoleCredentialName(IAM_ROLE_CREDENTIAL_NAME)
                .setColonReplacement("#");

        DynamicConfigurationProvider provider = new S3SecurityMappingConfigurationProvider(mappingConfig);

        // matches prefix -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo/data/test.csv"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret"));

        // matches prefix exactly -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret"));

        // no role selected and mapping has no default role
        assertMappingFails(
                provider,
                path("s3://bar/test"),
                "No S3 role selected and mapping has no default role");

        // matches prefix and user selected one of allowed roles
        assertMapping(
                provider,
                path("s3://bar/test").withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_bucket_2"),
                role("arn:aws:iam::123456789101:role/allow_bucket_2"));

        // user selected role not in allowed list
        assertMappingFails(
                provider,
                path("s3://bar/test").withUser("bob").withExtraCredentialIamRole("bogus"),
                "Selected S3 role is not allowed: bogus");

        // verify that colon replacement works
        String roleWithoutColon = "arn#aws#iam##123456789101#role/allow_bucket_2";
        assertThat(roleWithoutColon).doesNotContain(":");
        assertMapping(
                provider,
                path("s3://bar/test").withExtraCredentialIamRole(roleWithoutColon),
                role("arn:aws:iam::123456789101:role/allow_bucket_2"));

        // matches prefix -- default role used
        assertMapping(
                provider,
                path("s3://bar/abc/data/test.csv"),
                role("arn:aws:iam::123456789101:role/allow_path"));

        // matches empty rule at end -- default role used
        assertMapping(
                provider,
                empty(),
                role("arn:aws:iam::123456789101:role/default"));

        // matches prefix -- default role used
        assertMapping(
                provider,
                path("s3://xyz/default"),
                role("arn:aws:iam::123456789101:role/allow_default"));

        // matches prefix and user selected one of allowed roles
        assertMapping(
                provider,
                path("s3://xyz/foo").withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_foo"),
                role("arn:aws:iam::123456789101:role/allow_foo"));

        // matches prefix and user selected one of allowed roles
        assertMapping(
                provider,
                path("s3://xyz/bar").withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_bar"),
                role("arn:aws:iam::123456789101:role/allow_bar"));

        // matches user -- default role used
        assertMapping(
                provider,
                empty().withUser("alice"),
                role("alice_role"));

        // matches user and user selected default role
        assertMapping(
                provider,
                empty().withUser("alice").withExtraCredentialIamRole("alice_role"),
                role("alice_role"));

        // matches user and selected role not allowed
        assertMappingFails(
                provider,
                empty().withUser("alice").withExtraCredentialIamRole("bogus"),
                "Selected S3 role is not allowed: bogus");

        // verify that first matching rule is used
        // matches prefix earlier in file and selected role not allowed
        assertMappingFails(
                provider,
                path("s3://bar/test").withUser("alice").withExtraCredentialIamRole("alice_role"),
                "Selected S3 role is not allowed: alice_role");

        // matches user regex -- default role used
        assertMapping(
                provider,
                empty().withUser("bob"),
                role("bob_and_charlie_role"));
    }

    private static void assertMapping(DynamicConfigurationProvider provider, MappingSelector selector, MappingResult mappingResult)
    {
        Configuration configuration = new Configuration(false);

        assertNull(configuration.get(S3_ACCESS_KEY));
        assertNull(configuration.get(S3_SECRET_KEY));
        assertNull(configuration.get(S3_IAM_ROLE));

        applyMapping(provider, selector, configuration);

        assertEquals(configuration.get(S3_ACCESS_KEY), mappingResult.getAccessKey().orElse(null));
        assertEquals(configuration.get(S3_SECRET_KEY), mappingResult.getSecretKey().orElse(null));
        assertEquals(configuration.get(S3_IAM_ROLE), mappingResult.getRole().orElse(null));
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
        provider.updateConfiguration(configuration, selector.getHdfsContext(), selector.getPath().toUri());
    }

    public static class MappingSelector
    {
        public static MappingSelector empty()
        {
            return path(DEFAULT_PATH);
        }

        public static MappingSelector path(String path)
        {
            return new MappingSelector(DEFAULT_USER, new Path(path), Optional.empty());
        }

        private final String user;
        private final Path path;
        private final Optional<String> extraCredentialIamRole;

        private MappingSelector(String user, Path path, Optional<String> extraCredentialIamRole)
        {
            this.user = requireNonNull(user, "user is null");
            this.path = requireNonNull(path, "path is null");
            this.extraCredentialIamRole = requireNonNull(extraCredentialIamRole, "extraCredentialIamRole is null");
        }

        public Path getPath()
        {
            return path;
        }

        public MappingSelector withExtraCredentialIamRole(String role)
        {
            return new MappingSelector(user, path, Optional.of(role));
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user, path, extraCredentialIamRole);
        }

        public HdfsContext getHdfsContext()
        {
            ImmutableMap.Builder<String, String> extraCredentials = ImmutableMap.builder();
            extraCredentialIamRole.ifPresent(role -> extraCredentials.put(IAM_ROLE_CREDENTIAL_NAME, role));

            ConnectorSession connectorSession = new TestingConnectorSession(
                    new ConnectorIdentity(
                            user, Optional.empty(), Optional.empty(), extraCredentials.build(), emptyMap()),
                    new HiveSessionProperties(
                            new HiveClientConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()
                    ).getSessionProperties());
            return new HdfsContext(connectorSession, "schema");
        }
    }

    public static class MappingResult
    {
        public static MappingResult credentials(String accessKey, String secretKey)
        {
            return new MappingResult(Optional.of(accessKey), Optional.of(secretKey), Optional.empty());
        }

        public static MappingResult role(String role)
        {
            return new MappingResult(Optional.empty(), Optional.empty(), Optional.of(role));
        }

        private final Optional<String> accessKey;
        private final Optional<String> secretKey;
        private final Optional<String> role;

        private MappingResult(Optional<String> accessKey, Optional<String> secretKey, Optional<String> role)
        {
            this.accessKey = requireNonNull(accessKey, "accessKey is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.role = requireNonNull(role, "role is null");
        }

        public Optional<String> getAccessKey()
        {
            return accessKey;
        }

        public Optional<String> getSecretKey()
        {
            return secretKey;
        }

        public Optional<String> getRole()
        {
            return role;
        }
    }
}
