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

import com.facebook.presto.hive.metastore.MetastoreConfig;
import com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingsSupplier;
import com.facebook.presto.hive.security.lakeformation.annotations.ForLakeFormationSecurity;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import java.util.concurrent.Executor;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class LakeFormationSecurityModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(LakeFormationSecurityConfig.class);
        configBinder(binder).bindConfig(GlueHiveMetastoreConfig.class);
        configBinder(binder).bindConfig(GlueSecurityMappingConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);
        binder.bind(ConnectorAccessControl.class).to(LakeFormationAccessControl.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForLakeFormationSecurity
    public GlueSecurityMappingsSupplier provideGlueSecurityMappingsSupplier(GlueSecurityMappingConfig config)
    {
        return new GlueSecurityMappingsSupplier(config.getConfigFile(), config.getRefreshPeriod());
    }

    @Provides
    @Singleton
    @ForLakeFormationSecurity
    public Executor createLFPolicyRefreshExecutor(LakeFormationSecurityConfig config)
    {
        return newFixedThreadPool(config.getMaxRefreshThreads(), daemonThreadsNamed("lake-formation-policy-refresh-%s"));
    }
}
