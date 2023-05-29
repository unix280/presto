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
import com.facebook.presto.spi.governance.QueryAuditAndGovernance;
import com.facebook.presto.spi.governance.QueryAuditAndGovernanceFactory;
import com.facebook.presto.spi.security.Identity;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DefaultQueryAuditAndGovernance
        implements QueryAuditAndGovernance
{
    public static final String NAME = "default";
    private static final DefaultQueryAuditAndGovernance INSTANCE = new DefaultQueryAuditAndGovernance();
    public static final String EMPTY_STRING = "";

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
     * This method is the implementation of governquery method for the governance type - default
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
        return EMPTY_STRING;
    }

    @Override
    public void auditquery(String query, String catalog, String schema, Identity identity)
    {}
}
