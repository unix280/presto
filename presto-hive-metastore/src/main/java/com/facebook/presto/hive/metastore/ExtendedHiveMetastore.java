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

public interface ExtendedHiveMetastore
{
    Optional<Database> getDatabase(HiveIdentity hiveIdentity, String databaseName);

    List<String> getAllDatabases(HiveIdentity hiveIdentity);

    Optional<Table> getTable(HiveIdentity hiveIdentity, String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, Set<String> partitionNames);

    void updateTableStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(HiveIdentity hiveIdentity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    Optional<List<String>> getAllTables(HiveIdentity hiveIdentity, String databaseName);

    Optional<List<String>> getAllViews(HiveIdentity hiveIdentity, String databaseName);

    void createDatabase(HiveIdentity hiveIdentity, Database database);

    void dropDatabase(HiveIdentity hiveIdentity, String databaseName);

    void renameDatabase(HiveIdentity hiveIdentity, String databaseName, String newDatabaseName);

    void createTable(HiveIdentity hiveIdentity, Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(HiveIdentity hiveIdentity, String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void replaceTable(HiveIdentity hiveIdentity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    void renameTable(HiveIdentity hiveIdentity, String databaseName, String tableName, String newDatabaseName, String newTableName);

    void addColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String oldColumnName, String newColumnName);

    void dropColumn(HiveIdentity hiveIdentity, String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> partitionValues);

    Optional<List<String>> getPartitionNames(HiveIdentity hiveIdentity, String databaseName, String tableName);

    List<String> getPartitionNamesByFilter(
            HiveIdentity hiveIdentity,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates);

    List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            HiveIdentity hiveIdentity,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates);

    Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> partitionNames);

    void addPartitions(HiveIdentity hiveIdentity, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(HiveIdentity hiveIdentity, String databaseName, String tableName, PartitionWithStatistics partition);

    void createRole(String role, String grantor);

    void dropRole(String role);

    Set<String> listRoles();

    void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor);

    void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor);

    Set<RoleGrant> listRoleGrants(PrestoPrincipal principal);

    void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal);

    boolean isImpersonationEnabled();
}
