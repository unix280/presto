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
package com.facebook.presto.governance.strategy;

import com.facebook.presto.governance.entity.GovernPolicyFormat;
import com.facebook.presto.governance.entity.QueryBasicInfo;
import com.facebook.presto.governance.strategy.external.ExternalGovernanceSystemClientFactory;
import com.facebook.presto.spi.security.Identity;

import java.util.List;

public class ExternalGovernancePolicyStrategy
        implements GovernancePolicyStrategy
{
    public static final String NAME = "external";
    private static final String EXTERNAL_SYSTEM_CLIENT = "wkc";

    //The above will be made as a configuration

    /**
     * This method will fetch the Policy Format from an External Governance System
     *
     * @param queryBasicInfo
     * @param catalog
     * @param schema
     * @param identity
     * @return
     */
    @Override
    public List<GovernPolicyFormat> getGovernancePolicy(QueryBasicInfo queryBasicInfo, String catalog, String schema, Identity identity)
    {
        List<GovernPolicyFormat> policyformatList = new ExternalGovernanceSystemClientFactory().getGovernSystem(EXTERNAL_SYSTEM_CLIENT).fetchrules(queryBasicInfo, catalog, schema, identity);
        return policyformatList;
    }
}
