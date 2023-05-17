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
package com.facebook.presto.governance;

import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.governance.entity.GovernPolicyFormat;
import com.facebook.presto.governance.entity.QueryBasicInfo;
import com.facebook.presto.governance.entity.QueryRewriteApplicableResponse;
import com.facebook.presto.governance.strategy.GovernancePolicyStrategyFactory;
import com.facebook.presto.governance.util.GovernanceUtil;
import com.facebook.presto.spi.governance.QueryAuditAndGovernance;
import com.facebook.presto.spi.governance.QueryAuditAndGovernanceFactory;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ExternalQueryAuditAndGovernance
        implements QueryAuditAndGovernance
{
    public static final String NAME = "external";
    private static final ExternalQueryAuditAndGovernance INSTANCE = new ExternalQueryAuditAndGovernance();

    public static class Factory
            implements QueryAuditAndGovernanceFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public QueryAuditAndGovernance create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");
            checkArgument(config.isEmpty(), "This governance controller does not support any configuration properties");
            return INSTANCE;
        }
    }

    /**
     * This method is the implementation of governquery method for the governance type - external
     *
     * @param preparedQuery
     * @param query
     * @param catalog
     * @param schema
     * @param identity
     * @return
     */
    @Override
    public String governquery(PreparedQuery preparedQuery, String query, String catalog, String schema, Identity identity)
    {
        String modifiedQuery = null;
        if (preparedQuery instanceof BuiltInQueryPreparer.BuiltInPreparedQuery && null != ((BuiltInQueryPreparer.BuiltInPreparedQuery) preparedQuery).getWrappedStatement()) {
            Statement wrappedStatement = ((BuiltInQueryPreparer.BuiltInPreparedQuery) preparedQuery).getWrappedStatement();
            if (wrappedStatement instanceof Query) {
                GovernanceUtil governanceUtil = new GovernanceUtil();
                QueryBasicInfo queryBasicInfo = governanceUtil.getQueryBasicInfo(preparedQuery, catalog, schema, identity);
                List<GovernPolicyFormat> policyformatList = new GovernancePolicyStrategyFactory().getStrategy(NAME).getGovernancePolicy(queryBasicInfo, catalog, schema, identity);
                //governanceUtil.enrichPolicyListWithSubActionMapping(policyformatList);
                if (null != policyformatList && !policyformatList.isEmpty()) {
                    QueryRewriteApplicableResponse queryRewriteApplicableResponse = governanceUtil.isQueryReWriteRequired(wrappedStatement, policyformatList, queryBasicInfo);
                    if (queryRewriteApplicableResponse.isRewriteApplicable()) {
                        governanceUtil.enrichColumnMaskInformation(queryRewriteApplicableResponse);
                        query = governanceUtil.enrichSelectAllColumnQuery(queryRewriteApplicableResponse, query);
                        modifiedQuery = governanceUtil.getModifiedRewrittenQuery(query, queryRewriteApplicableResponse.getApplicablePolicies(), queryBasicInfo);
                    }
                }
            }
        }
        return modifiedQuery;
    }

    @Override
    public void auditquery(String query, String catalog, String schema, Identity identity)
    {}
}
