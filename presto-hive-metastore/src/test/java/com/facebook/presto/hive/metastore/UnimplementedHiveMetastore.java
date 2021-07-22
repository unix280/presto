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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.authentication.HiveIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class UnimplementedHiveMetastore
        implements ExtendedHiveMetastore
{
    @Override
    public Optional<Database> getDatabase(HiveIdentity hiveIdentity, String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllDatabases(HiveIdentity hiveIdentity)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Table> getTable(HiveIdentity hiveIdentity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, Set<String> partitionNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTableStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<String>> getAllTables(HiveIdentity hiveIdentity, String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<String>> getAllViews(HiveIdentity hiveIdentity, String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(HiveIdentity hiveIdentity, Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(HiveIdentity hiveIdentity, String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameDatabase(HiveIdentity hiveIdentity, String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(HiveIdentity hiveIdentity, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(HiveIdentity hiveIdentity, String databaseName, String tableName, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceTable(HiveIdentity hiveIdentity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(HiveIdentity hiveIdentity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> partitionValues)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity hiveIdentity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            HiveIdentity hiveIdentity, String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            HiveIdentity hiveIdentity, String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> partitionNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPartitions(HiveIdentity hiveIdentity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }
}
