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

import com.facebook.airlift.configuration.ConfigurationFactory;
import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.hive.security.lakeformation.LakeFormationSecurityConfig.LAKEFORMATION_POLICY_CACHE_TTL;
import static com.facebook.presto.hive.security.lakeformation.LakeFormationSecurityConfig.LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS;
import static com.facebook.presto.hive.security.lakeformation.LakeFormationSecurityConfig.LAKE_FORMATION_POLICY_REFRESH_PERIOD;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLakeFormationSecurityConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(LakeFormationSecurityConfig.class)
                .setCacheTTL(new Duration(2, HOURS))
                .setRefreshPeriod(new Duration(5, MINUTES))
                .setMaxRefreshThreads(1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put(LAKEFORMATION_POLICY_CACHE_TTL, "3h")
                .put(LAKE_FORMATION_POLICY_REFRESH_PERIOD, "1m")
                .put(LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS, "5")
                .build();

        LakeFormationSecurityConfig expected = new LakeFormationSecurityConfig()
                .setCacheTTL(new Duration(3, HOURS))
                .setRefreshPeriod(new Duration(1, MINUTES))
                .setMaxRefreshThreads(5);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                LAKEFORMATION_POLICY_CACHE_TTL, "30s",
                LAKE_FORMATION_POLICY_REFRESH_PERIOD, "30s",
                LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS, "1")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property " + LAKEFORMATION_POLICY_CACHE_TTL);

        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                LAKEFORMATION_POLICY_CACHE_TTL, "60s",
                LAKE_FORMATION_POLICY_REFRESH_PERIOD, "10s",
                LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS, "1")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property " + LAKE_FORMATION_POLICY_REFRESH_PERIOD);

        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                LAKEFORMATION_POLICY_CACHE_TTL, "60s",
                LAKE_FORMATION_POLICY_REFRESH_PERIOD, "30s",
                LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS, "0")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property " + LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS);
    }

    private static LakeFormationSecurityConfig newInstance(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        return configurationFactory.build(LakeFormationSecurityConfig.class);
    }
}
