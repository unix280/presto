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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.spi.governance.QueryAuditAndGovernance;
import com.facebook.presto.spi.governance.QueryAuditAndGovernanceFactory;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryAuditAndGovernanceManager
        implements QueryAuditAndGovernance
{
    private static final Logger log = Logger.get(QueryAuditAndGovernanceManager.class);
    private static final File QUERY_GOVERNANCE_CONFIGURATION = new File("etc/query-governance.properties");
    private static final String QUERY_GOVERNANCE_PROPERTY_NAME = "query-governance.name";
    public static final String EMPTY_STRING = "";
    private final Map<String, QueryAuditAndGovernanceFactory> queryAuditAndGovernanceFactories = new ConcurrentHashMap<>();
    private final AtomicReference<QueryAuditAndGovernance> qualityAuditAndGovernance = new AtomicReference<>(new InitializingQueryAuditAndGovernance());
    private final AtomicBoolean qualityAuditAndGovernanceLoading = new AtomicBoolean();

    /**
     * This method is the manager implementation for QAAD Plugin
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
        return qualityAuditAndGovernance.get().governquery(preparedQuery, query, catalog, schema, identity);
    }

    @Override
    public void auditquery(String query, String catalog, String schema, Identity identity)
    {}

    @Inject
    public QueryAuditAndGovernanceManager(TransactionManager transactionManager)
    {
        addQueryAuditAndGovernanceFactory(new DefaultQueryAuditAndGovernance.Factory());
        addQueryAuditAndGovernanceFactory(new ExternalQueryAuditAndGovernance.Factory());
        addQueryAuditAndGovernanceFactory(new StandardQueryAuditAndGovernance.Factory());
    }

    /**
     * This method will add Query Audit and Governance Factory
     *
     * @param queryAuditAndGovernanceFactory
     */
    public void addQueryAuditAndGovernanceFactory(QueryAuditAndGovernanceFactory queryAuditAndGovernanceFactory)
    {
        requireNonNull(queryAuditAndGovernanceFactory, "queryAuditAndGovernanceFactory is null");

        if (queryAuditAndGovernanceFactories.putIfAbsent(queryAuditAndGovernanceFactory.getName(), queryAuditAndGovernanceFactory) != null) {
            throw new IllegalArgumentException(format("Query Audit And Governance '%s' is already registered", queryAuditAndGovernanceFactory.getName()));
        }
    }

    /**
     * This method will laad Query Audit And Governance Configurations
     *
     * @throws Exception
     */
    public void loadQueryAuditAndGovernance()
            throws Exception
    {
        if (QUERY_GOVERNANCE_CONFIGURATION.exists()) {
            Map<String, String> properties = loadProperties(QUERY_GOVERNANCE_CONFIGURATION);
            checkArgument(!isNullOrEmpty(properties.get(QUERY_GOVERNANCE_PROPERTY_NAME)),
                    "Access control configuration %s does not contain %s",
                    QUERY_GOVERNANCE_CONFIGURATION.getAbsoluteFile(),
                    QUERY_GOVERNANCE_PROPERTY_NAME);

            loadQueryAuditAndGovernance(properties);
        }
        else {
            setQueryAuditAndGovernance(DefaultQueryAuditAndGovernance.NAME, ImmutableMap.of());
        }
    }

    /**
     * This method will load QAAD Properties from the governance properties file
     *
     * @param properties
     */
    public void loadQueryAuditAndGovernance(Map<String, String> properties)
    {
        properties = new HashMap<>(properties);
        String accessControlName = properties.remove(QUERY_GOVERNANCE_PROPERTY_NAME);
        checkArgument(!isNullOrEmpty(accessControlName), "%s property must be present", QUERY_GOVERNANCE_PROPERTY_NAME);

        setQueryAuditAndGovernance(accessControlName, properties);
    }

    /**
     * This method will initialise the QAAD Object Class based on the configuration in the properties file
     *
     * @param name
     * @param properties
     */
    @VisibleForTesting
    protected void setQueryAuditAndGovernance(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        checkState(qualityAuditAndGovernanceLoading.compareAndSet(false, true), "Query Audit And Governance already initialized");

        log.info("-- Loading Query Audit And Governance --");

        QueryAuditAndGovernanceFactory queryAuditAndGovernanceFactory = queryAuditAndGovernanceFactories.get(name);
        checkState(queryAuditAndGovernanceFactory != null, "QueryAuditAndGovernance %s is not registered", name);

        QueryAuditAndGovernance queryAuditAndGovernance = queryAuditAndGovernanceFactory.create(ImmutableMap.copyOf(properties));
        this.qualityAuditAndGovernance.set(queryAuditAndGovernance);

        log.info("-- Loaded Query Audit And Governance %s --", name);
    }

    private static class InitializingQueryAuditAndGovernance
            implements QueryAuditAndGovernance
    {
        /**
         * This method will be called during the presto server is being initialized
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
            //throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
            return EMPTY_STRING;
        }

        @Override
        public void auditquery(String query, String catalog, String schema, Identity identity)
        {
        }
    }
}
