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

import com.facebook.presto.hive.authentication.HiveIdentity;
import com.facebook.presto.spi.security.ConnectorIdentity;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.metastore.glue.TestGlueSecurityMappings.MappingSelector.empty;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestGlueSecurityMappings
{
    private static final String DEFAULT_USER = "defaultUser";

    @Test
    public void testMapping()
    {
        GlueSecurityMapping adminUser = new GlueSecurityMapping(Optional.of(Pattern.compile("admin")), Optional.of("arn:aws:iam::123456789101:role/admin_role"));
        GlueSecurityMapping analystUser = new GlueSecurityMapping(Optional.of(Pattern.compile("analyst")), Optional.of("arn:aws:iam::123456789101:role/analyst_role"));
        GlueSecurityMapping defaultUser = new GlueSecurityMapping(Optional.empty(), Optional.of("arn:aws:iam::123456789101:role/default_role"));

        List<GlueSecurityMapping> mappingList = new ArrayList<>();
        mappingList.add(adminUser);
        mappingList.add(analystUser);
        mappingList.add(defaultUser);

        GlueSecurityMappings mappings = new GlueSecurityMappings(mappingList);

        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/admin_role").getIamRole(),
                mappings.getMapping(empty().withUser("admin").getHiveIdentity()).get().getIamRole());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/analyst_role").getIamRole(),
                mappings.getMapping(empty().withUser("analyst").getHiveIdentity()).get().getIamRole());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/default_role").getIamRole(),
                mappings.getMapping(empty().withUser("anyUser").getHiveIdentity()).get().getIamRole());
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

        public HiveIdentity getHiveIdentity()
        {
            return new HiveIdentity(new ConnectorIdentity(
                    user, Optional.empty(), Optional.empty(), Collections.emptyMap(), Collections.emptyMap()));
        }
    }

    public static class MappingResult
    {
        public static MappingResult role(String role)
        {
            return new MappingResult(role);
        }

        private final String iamRole;

        private MappingResult(String iamRole)
        {
            this.iamRole = requireNonNull(iamRole, "role is null");;
        }

        public String getIamRole()
        {
            return iamRole;
        }
    }
}
