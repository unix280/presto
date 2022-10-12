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
package com.facebook.presto.hive.security.ranger;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.JsonResponse;
import com.facebook.presto.client.OkHttpUtil;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.jetbrains.annotations.TestOnly;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_POLICY_MGR_DOWNLOAD_URL;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_GROUP_URL;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_ROLES_URL;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyShowColumnsMetadata;
import static com.facebook.presto.spi.security.AccessDeniedException.denyShowCreateTable;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Connector access control which uses existing Ranger policies for authorizations
 */

public class RangerBasedAccessControl
        implements ConnectorAccessControl
{
    private static final Logger log = Logger.get(RangerBasedAccessControl.class);

    private RangerAuthorizer rangerAuthorizer;
    private Map<String, Set<String>> userGroupsMapping = new HashMap<>();
    private Map<String, Set<String>> userRolesMapping = new HashMap<>();

    @TestOnly
    public RangerBasedAccessControl()
    {
    }

    @Inject
    public RangerBasedAccessControl(RangerBasedAccessControlConfig config)
    {
        requireNonNull(config.getRangerHttpEndPoint(), "Ranger service http end point is null");
        requireNonNull(config.getRangerHiveServiceName(), "Ranger hive service name is null");

        ServicePolicies servicePolicies;
        try {
            OkHttpClient client = getAuthHttpClient(config);

            long startTime = System.nanoTime();
            servicePolicies = getHiveServicePolicies(client, config);
            log.debug("Policy retrieval time (milliseconds) : " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
            startTime = System.nanoTime();
            userGroupsMapping = getUserGroupsMappings(client, config.getRangerHttpEndPoint());
            log.debug("User Group retrieval time (milliseconds) : " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
            rangerAuthorizer = new RangerAuthorizer(servicePolicies, config);
            startTime = System.nanoTime();
            userRolesMapping = getRolesForUserList(client, config.getRangerHttpEndPoint());
            log.debug("Roles retrieval time (milliseconds) : " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));

            log.info("Retrieved policies count " + servicePolicies.getPolicies().size());
        }
        catch (Exception e) {
            throw new RuntimeException("Exception while querying ranger service ", e);
        }
    }

    private ServicePolicies getHiveServicePolicies(OkHttpClient client, RangerBasedAccessControlConfig config)
            throws IOException
    {
        HttpUrl hiveServicePolicyUrl = requireNonNull(HttpUrl.get(uriBuilderFrom(URI.create(config.getRangerHttpEndPoint()))
                .appendPath(RANGER_REST_POLICY_MGR_DOWNLOAD_URL + "/" + config.getRangerHiveServiceName())
                .build()));
        Response response = doRequest(client, hiveServicePolicyUrl);
        if (!response.isSuccessful()) {
            throw new RuntimeException(format("Request to %s failed: [Error: %s]", hiveServicePolicyUrl, response));
        }

        return jsonParse(response, ServicePolicies.class);
    }

    private List<VXUser> getUsers(OkHttpClient client, String rangerEndPoint)
    {
        ImmutableList.Builder<VXUser> vXUsers = ImmutableList.builder();
        long curIndex = 0;
        JsonResponse<Users> users;
        JsonCodec<Users> usersJsonCodec = jsonCodec(Users.class);
        do {
            HttpUrl getUsersUrl = requireNonNull(HttpUrl.get(uriBuilderFrom(URI.create(rangerEndPoint))
                    .appendPath(RANGER_REST_USER_GROUP_URL)
                    .addParameter("startIndex", Long.toString(curIndex))
                    .build()));

            Request request = new Request.Builder().url(getUsersUrl).header("Accept", "application/json").build();
            users = JsonResponse.execute(usersJsonCodec, client, request);
            if (!users.hasValue()) {
                throw new RuntimeException(format("Request to %s failed: %s [Error: %s]", getUsersUrl, users, users.getResponseBody()));
            }
            vXUsers.addAll(users.getValue().getvXUsers());
            curIndex += users.getValue().getPageSize();
        } while (curIndex < users.getValue().getTotalCount());

        return vXUsers.build();
    }

    private Map<String, Set<String>> getRolesForUserList(OkHttpClient client, String rangerEndPoint)
    {
        List<VXUser> users = getUsers(client, rangerEndPoint);
        List<String> usersList = users.stream().map(VXUser::getName).collect(toImmutableList());
        HttpUrl getRolesUrl;
        for (String user : usersList) {
            getRolesUrl = requireNonNull(HttpUrl.get(uriBuilderFrom(URI.create(rangerEndPoint))
                    .appendPath(RANGER_REST_USER_ROLES_URL + "/" + user)
                    .build()));
            userRolesMapping.put(user, getRolesForUser(client, getRolesUrl));
        }
        return userRolesMapping;
    }

    private Set<String> getRolesForUser(OkHttpClient client, HttpUrl endPtUri)
    {
        Request request = new Request.Builder().url(endPtUri).header("Accept", "application/json").build();
        JsonCodec<List> rolesJsonCodec = jsonCodec(List.class);
        JsonResponse<List> roles = JsonResponse.execute(rolesJsonCodec, client, request);
        if (!roles.hasValue()) {
            throw new RuntimeException(format("Request to %s failed: %s [Error: %s]", endPtUri, roles, roles.getResponseBody()));
        }
        return new HashSet<>(roles.getValue());
    }

    @TestOnly
    public void setRangerAuthorizer(RangerAuthorizer rangerAuthorizer)
    {
        this.rangerAuthorizer = rangerAuthorizer;
    }

    @TestOnly
    public void setUserGroups(Map<String, Set<String>> userGroupsMapping)
    {
        this.userGroupsMapping = userGroupsMapping;
    }

    @TestOnly
    public void setUserRoles(Map<String, Set<String>> userRolesMapping)
    {
        this.userRolesMapping = userRolesMapping;
    }

    private static <T> T jsonParse(Response response, Class<T> clazz)
            throws IOException
    {
        if (response.body() != null) {
            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.body().byteStream()))) {
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                return mapper.readValue(bufferedReader, clazz);
            }
        }
        return null;
    }

    private Map<String, Set<String>> getUserGroupsMappings(OkHttpClient client, String rangerEndPoint)
    {
        List<VXUser> users = getUsers(client, rangerEndPoint);
        ImmutableMap.Builder<String, Set<String>> userGroupsMapping = ImmutableMap.builder();
        for (VXUser vxUser : users) {
            if (!(isNull(vxUser.getGroupNameList()) || vxUser.getGroupNameList().isEmpty())) {
                userGroupsMapping.put(vxUser.getName(), ImmutableSet.copyOf(vxUser.getGroupNameList()));
            }
        }
        return userGroupsMapping.build();
    }

    enum HiveAccessType
    {
        NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, ALL, ADMIN
    }

    private boolean checkAccess(ConnectorIdentity identity, SchemaTableName tableName, String column, HiveAccessType accessType)
    {
        return rangerAuthorizer.authorizeHiveResource(tableName.getSchemaName(), tableName.getTableName(), column,
                accessType.toString(), identity.getUser(), userGroupsMapping.get(identity.getUser()), userRolesMapping.get(identity.getUser()));
    }

    /**
     * Check if identity is allowed to create the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        if (!rangerAuthorizer.authorizeHiveResource(schemaName, null, null,
                HiveAccessType.CREATE.toString(), identity.getUser(), userGroupsMapping.get(identity.getUser()), userRolesMapping.get(identity.getUser()))) {
            denyCreateSchema(schemaName, format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s ] ", identity.getUser(), schemaName));
        }
    }

    /**
     * Check if identity is allowed to drop the specified schema in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
        if (!rangerAuthorizer.authorizeHiveResource(schemaName, null, null,
                HiveAccessType.DROP.toString(), identity.getUser(), userGroupsMapping.get(identity.getUser()), userRolesMapping.get(identity.getUser()))) {
            denyDropSchema(schemaName, format("Access denied - User [ %s ] does not have [DROP] " +
                    "privilege on [ %s ] ", identity.getUser(), schemaName));
        }
    }

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must handle filter all results for unauthorized users,
     * since there are multiple way to list schemas.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context)
    {
    }

    /**
     * Filter the list of schemas to those visible to the identity.
     */
    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<String> schemaNames)
    {
        Set<String> allowedSchemas = new HashSet<>();
        Set<String> groups = userGroupsMapping.get(identity.getUser());
        Set<String> roles = userRolesMapping.get(identity.getUser());

        for (String schema : schemaNames) {
            if (rangerAuthorizer.authorizeHiveResource(schema, null, null, RangerPolicyEngine.ANY_ACCESS, identity.getUser(), groups, roles)) {
                allowedSchemas.add(schema);
            }
        }
        return allowedSchemas;
    }

    /**
     * Check if identity is allowed to execute SHOW CREATE TABLE or SHOW CREATE VIEW.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.SELECT)) {
            denyShowCreateTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [SELECT] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to create the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.CREATE)) {
            denyCreateTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s ] ", identity.getUser(), tableName.getSchemaName()));
        }
    }

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Set<SchemaTableName> tableNames)
    {
        Set<SchemaTableName> allowedTables = new HashSet<>();
        Set<String> groups = userGroupsMapping.get(identity.getUser());
        Set<String> roles = userRolesMapping.get(identity.getUser());

        for (SchemaTableName table : tableNames) {
            if (rangerAuthorizer.authorizeHiveResource(table.getSchemaName(), table.getTableName(), null, RangerPolicyEngine.ANY_ACCESS, identity.getUser(), groups, roles)) {
                allowedTables.add(table);
            }
        }
        return allowedTables;
    }

    /**
     * Check if identity is allowed to show columns of tables by executing SHOW COLUMNS, DESCRIBE etc.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterColumns} method must filter all results for unauthorized users,
     * since there are multiple ways to list columns.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowColumnsMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.SELECT)) {
            denyShowColumnsMetadata(tableName.getTableName(), format("Access denied - User [ %s ] does not have [SELECT] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Filter the list of columns to those visible to the identity.
     */
    @Override
    public List<ColumnMetadata> filterColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    /**
     * Check if identity is allowed to add columns to the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyAddColumn(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to drop columns from the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyDropColumn(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyRenameColumn(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to select from the specified columns in a relation.  The column set can be empty.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        Set<String> deniedColumns = new HashSet<>();
        for (String column : columnNames) {
            if (!checkAccess(identity, tableName, column, HiveAccessType.SELECT)) {
                deniedColumns.add(column);
            }
        }
        if (deniedColumns.size() > 0) {
            throw new AccessDeniedException(format("User [ %s ] does not have [SELECT] " +
                    "privilege on all mentioned columns of [ %s/%s ]", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to drop the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.DROP)) {
            denyDropTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [DROP] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to rename the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.ALTER)) {
            denyRenameTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [ALTER] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
    {
    }

    /**
     * Check if identity is allowed to insert into the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.UPDATE)) {
            denyInsertTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [UPDATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to delete from the specified table in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.UPDATE)) {
            denyDeleteTable(tableName.getTableName(), format("Access denied - User [ %s ] does not have [UPDATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to create the specified view in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        if (!checkAccess(identity, viewName, null, HiveAccessType.CREATE)) {
            denyCreateView(viewName.getTableName(), format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), viewName.getSchemaName(), viewName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to drop the specified view in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
    {
        if (!checkAccess(identity, viewName, null, HiveAccessType.DROP)) {
            denyDropView(viewName.getTableName(), format("Access denied - User [ %s ] does not have [DROP] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), viewName.getSchemaName(), viewName.getTableName()));
        }
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified columns in a relation.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        if (!checkAccess(identity, tableName, null, HiveAccessType.CREATE)) {
            denyCreateView(tableName.getTableName(), format("Access denied - User [ %s ] does not have [CREATE] " +
                    "privilege on [ %s/%s ] ", identity.getUser(), tableName.getSchemaName(), tableName.getTableName()));
        }

        Set<String> deniedColumns = new HashSet<>();
        for (String column : columnNames) {
            if (!checkAccess(identity, tableName, column, HiveAccessType.SELECT)) {
                deniedColumns.add(column);
            }
        }
        if (deniedColumns.size() > 0) {
            denyCreateViewWithSelect(tableName.getTableName(), identity);
        }
    }

    /**
     * Check if identity is allowed to set the specified property in this catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String propertyName)
    {
    }

    private static Response doRequest(OkHttpClient httpClient, HttpUrl anyURL)
            throws IOException
    {
        Request request = new Request.Builder().url(anyURL).header("Accept", "application/json").build();
        Response response = httpClient.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }
        return response;
    }

    public static Interceptor basicAuth(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");

        String credential = Credentials.basic(user, password);
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build());
    }

    private static OkHttpClient getAuthHttpClient(RangerBasedAccessControlConfig controlConfig)
    {
        OkHttpClient httpClient = new OkHttpClient.Builder().build();
        OkHttpClient.Builder builder = httpClient.newBuilder();
        OkHttpUtil.setupSsl(builder, Optional.ofNullable(controlConfig.getRangerRestKeystorePath()),
                Optional.ofNullable(controlConfig.getRangerRestKeystorePwd()),
                Optional.ofNullable(controlConfig.getRangerRestTruststorePath()),
                Optional.ofNullable(controlConfig.getRangerRestTruststorePwd()));
        builder.addInterceptor(basicAuth(controlConfig.getBasicAuthUser(), controlConfig.getBasicAuthPassword()));
        return builder.build();
    }
}
