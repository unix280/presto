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
import com.facebook.presto.spi.security.Identity;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class StandardGovernancePolicyStrategy
        implements GovernancePolicyStrategy
{
    public static final String NAME = "standard";
    private static final File QUERY_GOVERNANCE_STANDARD_JSON_CONFIG = new File("etc/govern/standard-governance-config.json");

    /**
     * This method will return List of Governance Policies taken from the standard definition
     *
     * @param queryBasicInfo : QueryBasicInfo
     * @param catalog
     * @param schema
     * @param identity
     * @return : List<GovernPolicyFormat>
     */
    @Override
    public List<GovernPolicyFormat> getGovernancePolicy(QueryBasicInfo queryBasicInfo, String catalog, String schema, Identity identity)
    {
        List<GovernPolicyFormat> policyformatList = loadStandardGovernanceConfig(queryBasicInfo);
        return policyformatList;
    }

    /**
     * This method will load Standard Governance Policy Configuration
     *
     * @param queryBasicInfo : QueryBasicInfo
     * @return QaadGovernPolicy
     */
    public List<GovernPolicyFormat> loadStandardGovernanceConfig(QueryBasicInfo queryBasicInfo)
    {
        List<GovernPolicyFormat> defaultGovernPolicy = null;
        if (QUERY_GOVERNANCE_STANDARD_JSON_CONFIG.exists()) {
            String defaultConfig = readFileAsString(String.valueOf(QUERY_GOVERNANCE_STANDARD_JSON_CONFIG));
            //Gson g = new Gson();
            //defaultGovernPolicy = g.fromJson(defaultConfig, QaadGovernPolicy.class);
        }
        //Add Code to Convert Standard Policy to JSON String
        return defaultGovernPolicy;
    }

    /**
     * This method will read String data from a file and returns the same.
     *
     * @param file : Path of the file
     * @return : String data read from the file
     */
    public static String readFileAsString(String file)
    {
        try {
            return new String(Files.readAllBytes(Paths.get(file)));
        }
        catch (IOException e) {
            return null;
        }
    }
}
