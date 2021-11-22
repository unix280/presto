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

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.GetUnfilteredTableMetadataResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingConfig;
import com.facebook.presto.hive.metastore.glue.GlueSecurityMappingsSupplier;
import com.facebook.presto.hive.security.lakeformation.LakeFormationAccessControl.LFPolicyCacheKey;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.security.Privilege.SELECT;
import static com.google.common.io.Resources.getResource;
import static org.testng.Assert.assertThrows;

@Test(singleThreaded = true)
public class TestLakeFormationAccessControl
{
    public static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};
    public static final AccessControlContext CONTEXT = new AccessControlContext(new QueryId("query_id"), Optional.empty(), Optional.empty());

    private static final String ADMIN_IAM_ROLE = "arn:aws:iam::789986721738:role/test_admin_role";
    private static final String ANALYST_IAM_ROLE = "arn:aws:iam::789986721738:role/test_analyst_role";
    private static final String DEFAULT_IAM_ROLE = "arn:aws:iam::789986721738:role/test_default_role";

    private final Map<LFPolicyCacheKey, Optional<GetUnfilteredTableMetadataResult>> mockGetUnfilteredTable = new HashMap<>();
    private ConnectorAccessControl lakeFormationAccessControl;

    /*
    We will be creating a mock Map which will be containing sample
    GetUnfilteredTableResult objects for different LFPolicyCacheKey keys.
    This map will act as a substitute for AWS API calls for fetching
    Lake Formation Permissions.
    */
    @BeforeClass
    public void setUp()
    {
        this.lakeFormationAccessControl = createLakeFormationAccessControl();

        // Create table customer
        Table customerTable = new Table();
        StorageDescriptor customerSD = new StorageDescriptor();
        List<Column> customerColumns = new ArrayList<>();
        customerColumns.add(new Column().withName("custkey").withType("INTEGER"));
        customerColumns.add(new Column().withName("name").withType("VARCHAR"));
        customerColumns.add(new Column().withName("address").withType("VARCHAR"));
        customerColumns.add(new Column().withName("nation").withType("VARCHAR"));
        customerColumns.add(new Column().withName("phone").withType("BIGINT"));
        customerColumns.add(new Column().withName("acctbal").withType("BIGINT"));
        customerColumns.add(new Column().withName("mktsegment").withType("VARCHAR"));
        customerSD.setColumns(customerColumns);
        customerTable.setStorageDescriptor(customerSD);

        // Create table orders
        Table ordersTable = new Table();
        StorageDescriptor ordersSD = new StorageDescriptor();
        List<Column> ordersColumns = new ArrayList<>();
        ordersColumns.add(new Column().withName("orderkey").withType("INTEGER"));
        ordersColumns.add(new Column().withName("custkey").withType("INTEGER"));
        ordersColumns.add(new Column().withName("orderstatus").withType("VARCHAR"));
        ordersColumns.add(new Column().withName("totalprice").withType("INTEGER"));
        ordersColumns.add(new Column().withName("orderdate").withType("DATE"));
        ordersColumns.add(new Column().withName("order-priority").withType("VARCHAR"));
        ordersSD.setColumns(ordersColumns);
        ordersTable.setStorageDescriptor(ordersSD);

        // User: admin, Database: test, Table: customer, Access: All columns
        LFPolicyCacheKey adminCustomerLFPolicyCacheKey = new LFPolicyCacheKey(new SchemaTableName("test", "customer"), ADMIN_IAM_ROLE);
        GetUnfilteredTableMetadataResult adminCustomerGetUnfilteredTableResult =
                new GetUnfilteredTableMetadataResult()
                        .withTable(customerTable)
                        .withIsRegisteredWithLakeFormation(true)
                        .withAuthorizedColumns("custkey", "name", "address", "nation", "phone", "acctbal", "mktsegment");

        this.mockGetUnfilteredTable.put(adminCustomerLFPolicyCacheKey, Optional.of(adminCustomerGetUnfilteredTableResult));

        // User: admin, Database: test, Table: orders, Access: All columns
        LFPolicyCacheKey adminOrdersLFPolicyCacheKey = new LFPolicyCacheKey(new SchemaTableName("test", "orders"), ADMIN_IAM_ROLE);
        GetUnfilteredTableMetadataResult adminOrdersGetUnfilteredTableResult =
                new GetUnfilteredTableMetadataResult()
                        .withTable(ordersTable)
                        .withIsRegisteredWithLakeFormation(true)
                        .withAuthorizedColumns("orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "order-priority");

        this.mockGetUnfilteredTable.put(adminOrdersLFPolicyCacheKey, Optional.of(adminOrdersGetUnfilteredTableResult));

        // User: analyst, Database: test, Table: customer, Access: Columns[custkey, name, nation, mktsegment]
        LFPolicyCacheKey analystCustomerLFPolicyCacheKey = new LFPolicyCacheKey(new SchemaTableName("test", "customer"), ANALYST_IAM_ROLE);
        GetUnfilteredTableMetadataResult analystCustomerGetUnfilteredTableResult =
                new GetUnfilteredTableMetadataResult()
                        .withTable(customerTable)
                        .withIsRegisteredWithLakeFormation(true)
                        .withAuthorizedColumns("custkey", "name", "nation", "mktsegment");

        this.mockGetUnfilteredTable.put(analystCustomerLFPolicyCacheKey, Optional.of(analystCustomerGetUnfilteredTableResult));

        // User: analyst, Database: test, Table: orders, Access: Columns[orderkey, custkey, orderstatus, orderdate]
        LFPolicyCacheKey analystOrdersLFPolicyCacheKey = new LFPolicyCacheKey(new SchemaTableName("test", "orders"), ANALYST_IAM_ROLE);
        GetUnfilteredTableMetadataResult analystOrdersGetUnfilteredTableResult =
                new GetUnfilteredTableMetadataResult()
                        .withTable(ordersTable)
                        .withIsRegisteredWithLakeFormation(true)
                        .withAuthorizedColumns("orderkey", "custkey", "orderstatus", "orderdate");

        this.mockGetUnfilteredTable.put(analystOrdersLFPolicyCacheKey, Optional.of(analystOrdersGetUnfilteredTableResult));

        // User: anyuser, Database: test, Table: [customer, orders], Access: No columns
        LFPolicyCacheKey anyUserCustomerLFPolicyCacheKey = new LFPolicyCacheKey(new SchemaTableName("test", "customer"), DEFAULT_IAM_ROLE);
        LFPolicyCacheKey anyUserOrdersLFPolicyCacheKey = new LFPolicyCacheKey(new SchemaTableName("test", "orders"), DEFAULT_IAM_ROLE);

        GetUnfilteredTableMetadataResult anyUserCustomerGetUnfilteredTableResult =
                new GetUnfilteredTableMetadataResult()
                        .withTable(customerTable)
                        .withIsRegisteredWithLakeFormation(true)
                        .withAuthorizedColumns(new ArrayList<>());

        GetUnfilteredTableMetadataResult anyUserOrdersGetUnfilteredTableResult =
                new GetUnfilteredTableMetadataResult()
                        .withTable(ordersTable)
                        .withIsRegisteredWithLakeFormation(true)
                        .withAuthorizedColumns(new ArrayList<>());

        this.mockGetUnfilteredTable.put(anyUserCustomerLFPolicyCacheKey, Optional.of(anyUserCustomerGetUnfilteredTableResult));
        this.mockGetUnfilteredTable.put(anyUserOrdersLFPolicyCacheKey, Optional.of(anyUserOrdersGetUnfilteredTableResult));
    }

    @Test
    public void testDefaultDenyPermission()
    {
        assertDenied(() -> lakeFormationAccessControl.checkCanRevokeTablePrivilege(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                SELECT,
                new SchemaTableName("test", "orders"),
                new PrestoPrincipal(PrincipalType.ROLE, "role"),
                true));

        assertDenied(() -> lakeFormationAccessControl.checkCanGrantTablePrivilege(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                SELECT,
                new SchemaTableName("test", "orders"),
                new PrestoPrincipal(PrincipalType.ROLE, "role"),
                true));

        assertDenied(() -> lakeFormationAccessControl.checkCanCreateRole(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "role",
                Optional.empty()));

        assertDenied(() -> lakeFormationAccessControl.checkCanDropRole(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "role"));

        assertDenied(() -> lakeFormationAccessControl.checkCanGrantRoles(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                ImmutableSet.of("role"),
                ImmutableSet.of(new PrestoPrincipal(PrincipalType.ROLE, "role")),
                true,
                Optional.empty(),
                "hive"));

        assertDenied(() -> lakeFormationAccessControl.checkCanRevokeRoles(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                ImmutableSet.of("role"),
                ImmutableSet.of(new PrestoPrincipal(PrincipalType.ROLE, "role")),
                true,
                Optional.empty(),
                "hive"));

        assertDenied(() -> lakeFormationAccessControl.checkCanSetRole(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "role",
                "hive"));

        assertDenied(() -> lakeFormationAccessControl.checkCanShowRoles(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "hive"));

        assertDenied(() -> lakeFormationAccessControl.checkCanShowCurrentRoles(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "hive"));

        assertDenied(() -> lakeFormationAccessControl.checkCanShowRoleGrants(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "hive"));
    }

    @Test
    private void testDefaultAllowPermission()
    {
        // Schema level access checks
        lakeFormationAccessControl.checkCanCreateSchema(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "test");
        lakeFormationAccessControl.checkCanDropSchema(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "test");
        lakeFormationAccessControl.checkCanRenameSchema(TRANSACTION_HANDLE, user("anyuser"), CONTEXT, "test", "newTest");
        lakeFormationAccessControl.checkCanShowSchemas(TRANSACTION_HANDLE, user("anyuser"), CONTEXT);

        //Table level access checks
        lakeFormationAccessControl.checkCanCreateTable(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"));
        lakeFormationAccessControl.checkCanDropTable(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"));
        lakeFormationAccessControl.checkCanRenameTable(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"),
                new SchemaTableName("test", "newOrders"));
        lakeFormationAccessControl.checkCanShowTablesMetadata(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "test");

        // Column level access checks
        lakeFormationAccessControl.checkCanAddColumn(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"));
        lakeFormationAccessControl.checkCanDropColumn(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"));
        lakeFormationAccessControl.checkCanRenameColumn(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"));

        // DML Access Checks
        lakeFormationAccessControl.checkCanInsertIntoTable(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"));
        lakeFormationAccessControl.checkCanDeleteFromTable(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"));

        // View Access Checks
        lakeFormationAccessControl.checkCanCreateView(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "ordersView"));
        lakeFormationAccessControl.checkCanDropView(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "ordersView"));

        // Catalog Session Property Access Check
        lakeFormationAccessControl.checkCanSetCatalogSessionProperty(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                "property");
    }

    @Test
    private void testCheckCanSelectFromColumns()
    {
        // Allow if schema is information_schema
        lakeFormationAccessControl.checkCanSelectFromColumns(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("information_schema", "table"),
                ImmutableSet.of());

        // Allow if columns are Presto special columns
        lakeFormationAccessControl.checkCanSelectFromColumns(
                TRANSACTION_HANDLE,
                user("admin"),
                CONTEXT,
                new SchemaTableName("test", "customer"),
                ImmutableSet.of("$path"));

        // User: admin, full access to all tables
        lakeFormationAccessControl.checkCanSelectFromColumns(
                TRANSACTION_HANDLE,
                user("admin"),
                CONTEXT,
                new SchemaTableName("test", "customer"),
                ImmutableSet.of("custkey", "name", "address", "nation", "phone", "acctbal", "mktsegment"));

        // User: analyst, access to subset of columns
        lakeFormationAccessControl.checkCanSelectFromColumns(
                TRANSACTION_HANDLE,
                user("analyst"),
                CONTEXT,
                new SchemaTableName("test", "customer"),
                ImmutableSet.of("custkey", "name", "nation", "mktsegment"));
        assertDenied(() -> lakeFormationAccessControl.checkCanSelectFromColumns(
                TRANSACTION_HANDLE,
                user("analyst"),
                CONTEXT,
                new SchemaTableName("test", "orders"),
                ImmutableSet.of("orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "order-priority")));

        // Any other user, no access to any table
        assertDenied(() -> lakeFormationAccessControl.checkCanSelectFromColumns(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"),
                ImmutableSet.of("orderkey")));
    }

    @Test
    private void testCheckCanCreateViewWithSelectFromColumns()
    {
        // Allow if schema is information_schema
        lakeFormationAccessControl.checkCanCreateViewWithSelectFromColumns(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("information_schema", "table"),
                ImmutableSet.of());

        // User: admin, full access to all tables
        lakeFormationAccessControl.checkCanCreateViewWithSelectFromColumns(
                TRANSACTION_HANDLE,
                user("admin"),
                CONTEXT,
                new SchemaTableName("test", "customer"),
                ImmutableSet.of("custkey", "name", "address", "nation", "phone", "acctbal", "mktsegment"));

        // User: analyst, access to subset of columns
        lakeFormationAccessControl.checkCanCreateViewWithSelectFromColumns(
                TRANSACTION_HANDLE,
                user("analyst"),
                CONTEXT,
                new SchemaTableName("test", "customer"),
                ImmutableSet.of("custkey", "name", "nation", "mktsegment"));
        assertDenied(() -> lakeFormationAccessControl.checkCanCreateViewWithSelectFromColumns(
                TRANSACTION_HANDLE,
                user("analyst"),
                CONTEXT,
                new SchemaTableName("test", "orders"),
                ImmutableSet.of("orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "order-priority")));

        // Any other user, no access to any table
        assertDenied(() -> lakeFormationAccessControl.checkCanCreateViewWithSelectFromColumns(
                TRANSACTION_HANDLE,
                user("anyuser"),
                CONTEXT,
                new SchemaTableName("test", "orders"),
                ImmutableSet.of("orderkey")));
    }

    private static ConnectorIdentity user(String name)
    {
        return new ConnectorIdentity(name, Optional.empty(), Optional.empty());
    }

    private ConnectorAccessControl createLakeFormationAccessControl()
    {
        LakeFormationAccessControl lakeFormationAccessControl = new LakeFormationAccessControl();
        lakeFormationAccessControl.setCatalogIamRole(null);

        GlueSecurityMappingConfig glueSecurityMappingConfig = new GlueSecurityMappingConfig()
                .setConfigFile(new File(getResource("com.facebook.presto.hive.metastore.glue/glue-security-mapping.json").getPath()));
        GlueSecurityMappingsSupplier glueSecurityMappingsSupplier = new GlueSecurityMappingsSupplier(glueSecurityMappingConfig.getConfigFile(), Optional.empty());

        lakeFormationAccessControl.setMappings(glueSecurityMappingsSupplier.getMappingsSupplier());
        lakeFormationAccessControl.setImpersonationEnabled(true);
        lakeFormationAccessControl.setLakeFormationPolicyCache(
                CacheBuilder.newBuilder().build(CacheLoader.from(this::mockGetUnfilteredTableResult)));

        return lakeFormationAccessControl;
    }

    private Optional<GetUnfilteredTableMetadataResult> mockGetUnfilteredTableResult(LFPolicyCacheKey lfPolicyCacheKey)
    {
        if (mockGetUnfilteredTable.containsKey(lfPolicyCacheKey)) {
            return mockGetUnfilteredTable.get(lfPolicyCacheKey);
        }
        return Optional.empty();
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }
}
