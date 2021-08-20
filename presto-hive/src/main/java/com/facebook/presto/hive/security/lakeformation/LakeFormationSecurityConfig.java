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

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class LakeFormationSecurityConfig
{
    public static final String LAKEFORMATION_POLICY_CACHE_TTL = "hive.metastore.glue.lakeformation.policy-cache-ttl";
    public static final String LAKE_FORMATION_POLICY_REFRESH_PERIOD = "hive.metastore.glue.lakeformation.policy-refresh-period";
    public static final String LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS = "hive.metastore.glue.lakeformation.policy-refresh-max-threads";

    private Duration cacheTTL = new Duration(2, HOURS);
    private Duration refreshPeriod = new Duration(5, MINUTES);
    private int maxRefreshThreads = 1;

    @NotNull
    public Duration getCacheTTL()
    {
        return cacheTTL;
    }

    @MinDuration("0ms")
    @Config(LAKEFORMATION_POLICY_CACHE_TTL)
    public LakeFormationSecurityConfig setCacheTTL(Duration cacheTTL)
    {
        this.cacheTTL = cacheTTL;
        return this;
    }

    @NotNull
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @MinDuration("1ms")
    @Config(LAKE_FORMATION_POLICY_REFRESH_PERIOD)
    public LakeFormationSecurityConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    @Min(1)
    public int getMaxRefreshThreads()
    {
        return maxRefreshThreads;
    }

    @Config(LAKE_FORMATION_POLICY_REFRESH_MAX_THREADS)
    public LakeFormationSecurityConfig setMaxRefreshThreads(int maxRefreshThreads)
    {
        this.maxRefreshThreads = maxRefreshThreads;
        return this;
    }
}
