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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingsSupplier;
import com.facebook.presto.hive.s3.lakeformation.annotations.ForLFCredentialVending;
import com.facebook.presto.hive.security.SecurityConfig;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class LakeFormationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GlueSecurityMappingConfig.class);
        configBinder(binder).bindConfig(GlueHiveMetastoreConfig.class);

        if (buildConfigObject(SecurityConfig.class).getSecuritySystem().equals("lake-formation")) {
            newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().to(LakeFormationS3ConfigurationProvider.class).in(Scopes.SINGLETON);
        }
    }

    @Provides
    @Singleton
    @ForLFCredentialVending
    public GlueSecurityMappingsSupplier provideGlueSecurityMappingsSupplier(GlueSecurityMappingConfig config)
    {
        return new GlueSecurityMappingsSupplier(config.getConfigFile(), config.getRefreshPeriod());
    }
}
