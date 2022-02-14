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
package com.facebook.presto.sql.query;

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.tracing.TracingConfig;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFilterHideUnauthorizedColumnsSession
{
    @Test
    public void testDisableWhenEnabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setHideUnauthorizedColumns(true);
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig()));
        assertThatThrownBy(() -> sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_UNAUTHORIZED_COLUMNS, "false"))
                .hasMessage("hide_unauthorized_columns cannot be disabled with session property when it was enabled with configuration");
    }

    @Test
    public void testEnableWhenAlreadyEnabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setHideUnauthorizedColumns(true);
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig()));
        sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_UNAUTHORIZED_COLUMNS, "true");
    }

    @Test
    public void testDisableWhenAlreadyDisabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig()));
        sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_UNAUTHORIZED_COLUMNS, "false");
    }

    @Test
    public void testEnableWhenDisabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig()));
        sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_UNAUTHORIZED_COLUMNS, "true");
    }
}
