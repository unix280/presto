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
package com.facebook.presto.plugin.postgresql;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ColumnMapping;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.plugin.jdbc.SliceWriteFunction;
import com.facebook.presto.plugin.jdbc.StandardColumnMappings;
import com.facebook.presto.plugin.jdbc.WriteMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.esri.core.geometry.ogc.OGCGeometry.fromBinary;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.geospatial.GeometryUtils.wktFromJtsGeometry;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.serialize;
import static com.facebook.presto.geospatial.serde.JtsGeometrySerde.deserialize;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    protected final Type jsonType;
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";

    private static final JsonFactory JSON_FACTORY = new JsonFactoryBuilder().configure(CANONICALIZE_FIELD_NAMES, false).build();
    private static final ObjectMapper SORTED_MAPPER = new JsonObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    @Inject
    public PostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TypeManager typeManager)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new Driver(), config));
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    @Override
    public WriteMapping toWriteMapping(Type type)
    {
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", StandardColumnMappings.varbinaryWriteFunction());
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.JSON)) {
            return WriteMapping.sliceMapping("jsonb", jsonWriteFunction());
        }

        return super.toWriteMapping(type);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName();
        int columnSize = typeHandle.getColumnSize();

        switch (typeName) {
            case "uuid":
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
            case "geometry":
                return Optional.of(geometryColumnMapping());
            case "jsonb":
            case "json":
                return Optional.of(jsonColumnMapping());
        }
        return super.toPrestoType(session, typeHandle);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles) throws SQLException
    {
        ImmutableMap.Builder<String, String> columnExpressions = ImmutableMap.builder();
        for (JdbcColumnHandle column : columnHandles) {
            JdbcTypeHandle jdbcTypeHandle = column.getJdbcTypeHandle();
            if (jdbcTypeHandle.getJdbcTypeName().equalsIgnoreCase("geometry")) {
                String columnName = column.getColumnName();
                columnExpressions.put(columnName, "ST_AsBinary(\"" + columnName + "\")");
            }
        }
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                columnExpressions.build(),
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(tableMetadata, session, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            if (DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState())) {
                throw new PrestoException(ALREADY_EXISTS, e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    quoted(newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                jsonWriteFunction());
    }

    private SliceWriteFunction jsonWriteFunction()
    {
        return ((statement, index, value) -> {
            PGobject pgObject = new PGobject();
            pgObject.setType("json");
            pgObject.setValue(value.toStringUtf8());
            statement.setObject(index, pgObject);
        });
    }

    public static Slice jsonParse(Slice slice)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, slice)) {
            byte[] in = slice.getBytes();
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(in.length);
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert '%s' to JSON", slice.toStringUtf8()));
        }
    }

    public static JsonParser createJsonParser(JsonFactory factory, Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass an InputStreamReader instead.
        return factory.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }

    private ColumnMapping geometryColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                VARCHAR,
                (resultSet, columnIndex) -> getAsText(getGeomFromBinary(wrappedBuffer(resultSet.getBytes(columnIndex)))),
                geometryWriteFunction());
    }

    private SliceWriteFunction geometryWriteFunction()
    {
        //TODO: see if this can be implemented in some way.
        return ((statement, index, value) -> {
            throw new UnsupportedOperationException();
        });
    }

    private static Slice getAsText(Slice input)
    {
        return utf8Slice(wktFromJtsGeometry(deserialize(input)));
    }

    private static Slice getGeomFromBinary(Slice input)
    {
        requireNonNull(input, "input is null");
        OGCGeometry geometry;
        try {
            geometry = fromBinary(input.toByteBuffer().slice());
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid WKB", e);
        }
        geometry.setSpatialReference(null);
        return serialize(geometry);
    }
}
