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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestFilterUnauthorizedColumns
{
    private static final String CATALOG = "local";
    private static final String USER = "user";
    private static final String ADMIN = "admin";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setSystemProperty("hide_unauthorized_columns", "true")
            .setIdentity(new Identity(USER, Optional.empty()))
            .build();

    private QueryAssertions assertions;
    private TestingAccessControlManager accessControl;

    @BeforeClass
    public void init()
    {
        LocalQueryRunner runner = new LocalQueryRunner(SESSION, new FeaturesConfig().setHideUnauthorizedColumns(true));

        runner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());
        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @BeforeMethod
    public void beforeMethod()
    {
        accessControl.reset();
    }

    @Test
    public void testSelectBaseline()
    {
        // No filtering baseline
        assertions.assertQuery("SELECT * FROM nation WHERE name = 'FRANCE'",
                "VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('refully final requests. regular, ironi' AS VARCHAR(152)))");
    }

    @Test
    public void testSimpleTableSchemaFilter()
    {
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertions.assertQuery("SELECT * FROM nation WHERE name = 'FRANCE'", "VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");
    }

    @Test
    public void testDescribeBaseline()
    {
        MaterializedResult result = assertions.getQueryRunner().execute("DESCRIBE nation");
        List<MaterializedRow> rows = result.getMaterializedRows();

        assertTrue(rows.stream().anyMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    @Test
    public void testDescribe()
    {
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));

        MaterializedResult result = assertions.getQueryRunner().execute("DESCRIBE nation");
        List<MaterializedRow> rows = result.getMaterializedRows();

        assertFalse(rows.stream().anyMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    @Test
    public void testShowColumnsBaseline()
    {
        MaterializedResult result = assertions.getQueryRunner().execute("SHOW COLUMNS FROM nation");
        List<MaterializedRow> rows = result.getMaterializedRows();

        assertTrue(rows.stream().anyMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    @Test
    public void testShowColumns()
    {
        accessControl.deny(privilege("nation.comment", SELECT_COLUMN));

        MaterializedResult result = assertions.getQueryRunner().execute("SHOW COLUMNS FROM nation");
        List<MaterializedRow> rows = result.getMaterializedRows();

        assertFalse(rows.stream().anyMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    /**
     * Test filtering when columns are explicitly specified in SELECT
     */
    @Test
    public void testFilterExplicitSelect()
    {
        // Select the columns that are available to us explicitly
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertions.assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE name = 'FRANCE'", "VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");

        // Select all columns explicitly
        assertThatThrownBy(() -> assertions.getQueryRunner().execute("SELECT nationkey, name, regionkey, comment FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view local.tiny.nation");
    }

    @Test
    public void testRowFilterOnNotAccessibleColumnAllowed()
    {
        accessControl.rowFilter(new QualifiedObjectName(CATALOG, TINY_SCHEMA_NAME, "nation"),
                USER,
                new ViewExpression(ADMIN, Optional.of(CATALOG), Optional.of(TINY_SCHEMA_NAME), "comment IS NOT null"));
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertions.assertQuery("SELECT * FROM nation WHERE name = 'FRANCE'", "VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");
    }

    @Test
    public void testRowFilterOnNotAccessibleColumnNotAllowed()
    {
        accessControl.rowFilter(new QualifiedObjectName(CATALOG, TINY_SCHEMA_NAME, "nation"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of(TINY_SCHEMA_NAME), "comment IS NOT null"));
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.getQueryRunner().execute("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view local.tiny.nation");
    }

    @Test
    public void testMaskingWithCaseOnNotAccessibleColumnNotAllowed()
    {
        accessControl.columnMask(new QualifiedObjectName(CATALOG, TINY_SCHEMA_NAME, "nation"),
                "comment",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of(TINY_SCHEMA_NAME), "CASE nationkey WHEN 6 THEN 'masked-comment' ELSE comment END"));

        accessControl.deny(privilege(USER, "nation.nationkey", SELECT_COLUMN));

        assertThatThrownBy(() -> assertions.getQueryRunner().execute("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view local.tiny.nation");
    }

    @Test
    public void testMaskingWithCaseOnNotAccessibleColumnAllowed()
    {
        accessControl.columnMask(new QualifiedObjectName(CATALOG, TINY_SCHEMA_NAME, "nation"),
                "comment",
                USER,
                new ViewExpression(ADMIN, Optional.of(CATALOG), Optional.of(TINY_SCHEMA_NAME), "CASE nationkey WHEN 6 THEN 'masked-comment' ELSE comment END"));

        accessControl.deny(privilege(USER, "nation.nationkey", SELECT_COLUMN));

        assertions.assertQuery("SELECT * FROM nation WHERE name = 'FRANCE'", "VALUES (CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('masked-comment' AS VARCHAR(152)))");
        assertions.assertQuery("SELECT * FROM nation WHERE name = 'CANADA'", "VALUES (CAST('CANADA' AS VARCHAR(25)), BIGINT '1', CAST('eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold' AS VARCHAR(152)))");
    }

    @Test
    public void testPredicateOnUnauthorizedColumn()
    {
        accessControl.deny(privilege(USER, "nation.name", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.getQueryRunner().execute("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view local.tiny.nation");
    }

    @Test
    public void testJoinBaseline()
    {
        MaterializedResult result = assertions.getQueryRunner().execute("SELECT * FROM nation,customer WHERE customer.nationkey = nation.nationkey AND nation.name = 'FRANCE' AND customer.name='Customer#000001477'");
        List<MaterializedRow> rows = result.getMaterializedRows();

        assertEquals(rows.get(0).getField(11), "ites nag blithely alongside of the ironic accounts. accounts use. carefully silent deposits");
    }

    @Test
    public void testJoin()
    {
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));

        MaterializedResult result = assertions.getQueryRunner().execute("SELECT * FROM nation,customer WHERE customer.nationkey = nation.nationkey AND nation.name = 'FRANCE' AND customer.name='Customer#000001477'");
        List<MaterializedRow> rows = result.getMaterializedRows();

        assertEquals(rows.get(0).getFields().size(), 11);
    }

    @Test
    public void testConstantFields()
    {
        assertions.assertQuery("SELECT * FROM (SELECT 'test')", "VALUES ('test')");
    }

    @Test
    public void testFunctionFields()
    {
        assertions.assertQuery("SELECT * FROM (SELECT concat(name,'-test') FROM nation WHERE name = 'FRANCE')",
                "VALUES (CAST('FRANCE-test' AS VARCHAR))");
    }

    @Test
    public void testFunctionOnUnauthorizedColumn()
    {
        accessControl.deny(privilege(USER, "nation.name", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.getQueryRunner().execute("SELECT * FROM (SELECT concat(name,'-test') FROM nation WHERE name = 'FRANCE')"))
                .hasMessage("Access Denied: Cannot select from columns [name] in table or view local.tiny.nation");
    }
}
