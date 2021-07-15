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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.authentication.HiveIdentity;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.csvSchemaFields;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiPartition;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.isAvroTableWithSchemaSet;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.isCsvTable;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiDatabase;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiTable;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class BridgingHiveMetastore
        implements ExtendedHiveMetastore
{
    private final HiveMetastore delegate;
    private final PartitionMutator partitionMutator;

    @Inject
    public BridgingHiveMetastore(HiveMetastore delegate, PartitionMutator partitionMutator)
    {
        this.delegate = delegate;
        this.partitionMutator = partitionMutator;
    }

    @Override
    public Optional<Database> getDatabase(HiveIdentity hiveIdentity, String databaseName)
    {
        return delegate.getDatabase(hiveIdentity, databaseName).map(ThriftMetastoreUtil::fromMetastoreApiDatabase);
    }

    @Override
    public List<String> getAllDatabases(HiveIdentity hiveIdentity)
    {
        return delegate.getAllDatabases(hiveIdentity);
    }

    @Override
    public Optional<Table> getTable(HiveIdentity hiveIdentity, String databaseName, String tableName)
    {
        return delegate.getTable(hiveIdentity, databaseName, tableName).map(table -> {
            if (isAvroTableWithSchemaSet(table)) {
                return fromMetastoreApiTable(table, delegate.getFields(hiveIdentity, databaseName, tableName).get());
            }
            if (isCsvTable(table)) {
                return fromMetastoreApiTable(table, csvSchemaFields(table.getSd().getCols()));
            }
            return fromMetastoreApiTable(table);
        });
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName)
    {
        return delegate.getTableStatistics(hiveIdentity, databaseName, tableName);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, Set<String> partitionNames)
    {
        return delegate.getPartitionStatistics(hiveIdentity, databaseName, tableName, partitionNames);
    }

    @Override
    public void updateTableStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updateTableStatistics(hiveIdentity, databaseName, tableName, update);
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updatePartitionStatistics(hiveIdentity, databaseName, tableName, partitionName, update);
    }

    @Override
    public Optional<List<String>> getAllTables(HiveIdentity hiveIdentity, String databaseName)
    {
        return delegate.getAllTables(hiveIdentity, databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(HiveIdentity hiveIdentity, String databaseName)
    {
        return delegate.getAllViews(hiveIdentity, databaseName);
    }

    @Override
    public void createDatabase(HiveIdentity hiveIdentity, Database database)
    {
        delegate.createDatabase(hiveIdentity, toMetastoreApiDatabase(database));
    }

    @Override
    public void dropDatabase(HiveIdentity hiveIdentity, String databaseName)
    {
        delegate.dropDatabase(hiveIdentity, databaseName);
    }

    @Override
    public void renameDatabase(HiveIdentity hiveIdentity, String databaseName, String newDatabaseName)
    {
        org.apache.hadoop.hive.metastore.api.Database database = delegate.getDatabase(hiveIdentity, databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
        database.setName(newDatabaseName);
        delegate.alterDatabase(hiveIdentity, databaseName, database);

        delegate.getDatabase(hiveIdentity, databaseName).ifPresent(newDatabase -> {
            if (newDatabase.getName().equals(databaseName)) {
                throw new PrestoException(NOT_SUPPORTED, "Hive metastore does not support renaming schemas");
            }
        });
    }

    @Override
    public void createTable(HiveIdentity hiveIdentity, Table table, PrincipalPrivileges principalPrivileges)
    {
        checkArgument(!table.getTableType().equals(TEMPORARY_TABLE), "temporary tables must never be stored in the metastore");
        delegate.createTable(hiveIdentity, toMetastoreApiTable(table, principalPrivileges));
    }

    @Override
    public void dropTable(HiveIdentity hiveIdentity, String databaseName, String tableName, boolean deleteData)
    {
        delegate.dropTable(hiveIdentity, databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(HiveIdentity hiveIdentity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        checkArgument(!newTable.getTableType().equals(TEMPORARY_TABLE), "temporary tables must never be stored in the metastore");
        alterTable(hiveIdentity, databaseName, tableName, toMetastoreApiTable(newTable, principalPrivileges));
    }

    @Override
    public void renameTable(HiveIdentity hiveIdentity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(hiveIdentity, databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        table.setDbName(newDatabaseName);
        table.setTableName(newTableName);
        alterTable(hiveIdentity, databaseName, tableName, table);
    }

    @Override
    public void addColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(hiveIdentity, databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        table.getSd().getCols().add(
                new FieldSchema(columnName, columnType.getHiveTypeName().toString(), columnComment));
        alterTable(hiveIdentity, databaseName, tableName, table);
    }

    @Override
    public void renameColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(hiveIdentity, databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        for (FieldSchema fieldSchema : table.getPartitionKeys()) {
            if (fieldSchema.getName().equals(oldColumnName)) {
                throw new PrestoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
            }
        }
        for (FieldSchema fieldSchema : table.getSd().getCols()) {
            if (fieldSchema.getName().equals(oldColumnName)) {
                fieldSchema.setName(newColumnName);
            }
        }
        alterTable(hiveIdentity, databaseName, tableName, table);
    }

    @Override
    public void dropColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, hiveIdentity, databaseName, tableName, columnName);
        org.apache.hadoop.hive.metastore.api.Table table = delegate.getTable(hiveIdentity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        table.getSd().getCols().removeIf(fieldSchema -> fieldSchema.getName().equals(columnName));
        alterTable(hiveIdentity, databaseName, tableName, table);
    }

    private void alterTable(HiveIdentity hiveIdentity, String databaseName, String tableName, org.apache.hadoop.hive.metastore.api.Table table)
    {
        delegate.alterTable(hiveIdentity, databaseName, tableName, table);
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> partitionValues)
    {
        return delegate.getPartition(hiveIdentity, databaseName, tableName, partitionValues).map(partition -> fromMetastoreApiPartition(partition, partitionMutator));
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity hiveIdentity, String databaseName, String tableName)
    {
        return delegate.getPartitionNames(hiveIdentity, databaseName, tableName);
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            HiveIdentity hiveIdentity,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesByFilter(hiveIdentity, databaseName, tableName, partitionPredicates);
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            HiveIdentity hiveIdentity,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesWithVersionByFilter(hiveIdentity, databaseName, tableName, partitionPredicates);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(Collectors.toMap(identity(), MetastoreUtil::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = delegate.getPartitionsByNames(hiveIdentity, databaseName, tableName, partitionNames).stream()
                .map(partition -> fromMetastoreApiPartition(partition, partitionMutator))
                .collect(Collectors.toMap(Partition::getValues, identity()));
        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.build();
    }

    @Override
    public void addPartitions(HiveIdentity hiveIdentity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        delegate.addPartitions(hiveIdentity, databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        delegate.dropPartition(hiveIdentity, databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        delegate.alterPartition(hiveIdentity, databaseName, tableName, partition);
    }

    @Override
    public void createRole(String role, String grantor)
    {
        delegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        delegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        delegate.grantRoles(roles, grantees, withAdminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        delegate.revokeRoles(roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(PrestoPrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, principal);
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return delegate.isImpersonationEnabled();
    }
}
