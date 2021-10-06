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
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.MetastoreContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.metastore.CachingHiveMetastore.MetastoreCacheScope.ALL;
import static com.facebook.presto.hive.metastore.HivePartitionName.hivePartitionName;
import static com.facebook.presto.hive.metastore.HiveTableName.hiveTableName;
import static com.facebook.presto.hive.metastore.PartitionFilter.partitionFilter;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Streams.stream;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
        implements ExtendedHiveMetastore
{
    public enum MetastoreCacheScope
    {
        ALL, PARTITION
    }

    protected final ExtendedHiveMetastore delegate;
    private final LoadingCache<KeyAndContext<String>, Optional<Database>> databaseCache;
    private final LoadingCache<KeyAndContext<String>, List<String>> databaseNamesCache;
    private final LoadingCache<KeyAndContext<HiveTableName>, Optional<Table>> tableCache;
    private final LoadingCache<KeyAndContext<String>, Optional<List<String>>> tableNamesCache;
    private final LoadingCache<KeyAndContext<HiveTableName>, PartitionStatistics> tableStatisticsCache;
    private final LoadingCache<KeyAndContext<HivePartitionName>, PartitionStatistics> partitionStatisticsCache;
    private final LoadingCache<KeyAndContext<String>, Optional<List<String>>> viewNamesCache;
    private final LoadingCache<KeyAndContext<HivePartitionName>, Optional<Partition>> partitionCache;
    private final LoadingCache<KeyAndContext<PartitionFilter>, List<String>> partitionFilterCache;
    private final LoadingCache<KeyAndContext<HiveTableName>, Optional<List<String>>> partitionNamesCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<PrestoPrincipal, Set<RoleGrant>> roleGrantsCache;

    private final boolean partitionVersioningEnabled;

    @Inject
    public CachingHiveMetastore(
            @ForCachingHiveMetastore ExtendedHiveMetastore delegate,
            @ForCachingHiveMetastore ExecutorService executor,
            MetastoreClientConfig metastoreClientConfig)
    {
        this(
                delegate,
                executor,
                metastoreClientConfig.getMetastoreCacheTtl(),
                metastoreClientConfig.getMetastoreRefreshInterval(),
                metastoreClientConfig.getMetastoreCacheMaximumSize(),
                metastoreClientConfig.isPartitionVersioningEnabled(),
                metastoreClientConfig.getMetastoreCacheScope());
    }

    public CachingHiveMetastore(
            ExtendedHiveMetastore delegate,
            ExecutorService executor,
            Duration cacheTtl,
            Duration refreshInterval,
            long maximumSize,
            boolean partitionVersioningEnabled,
            MetastoreCacheScope metastoreCacheScope)
    {
        this(
                delegate,
                executor,
                OptionalLong.of(cacheTtl.toMillis()),
                refreshInterval.toMillis() >= cacheTtl.toMillis() ? OptionalLong.empty() : OptionalLong.of(refreshInterval.toMillis()),
                maximumSize,
                partitionVersioningEnabled,
                metastoreCacheScope);
    }

    public static CachingHiveMetastore memoizeMetastore(ExtendedHiveMetastore delegate, long maximumSize)
    {
        return new CachingHiveMetastore(
                delegate,
                newDirectExecutorService(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                maximumSize,
                false,
                ALL);
    }

    private CachingHiveMetastore(
            ExtendedHiveMetastore delegate,
            ExecutorService executor,
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMills,
            long maximumSize,
            boolean partitionVersioningEnabled,
            MetastoreCacheScope metastoreCacheScope)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");
        this.partitionVersioningEnabled = partitionVersioningEnabled;

        OptionalLong cacheExpiresAfterWriteMillis;
        OptionalLong cacheRefreshMills;
        long cacheMaxSize;

        OptionalLong partitionCacheExpiresAfterWriteMillis;
        OptionalLong partitionCacheRefreshMills;
        long partitionCacheMaxSize;

        switch (metastoreCacheScope) {
            case PARTITION:
                partitionCacheExpiresAfterWriteMillis = expiresAfterWriteMillis;
                partitionCacheRefreshMills = refreshMills;
                partitionCacheMaxSize = maximumSize;
                cacheExpiresAfterWriteMillis = OptionalLong.of(0);
                cacheRefreshMills = OptionalLong.of(0);
                cacheMaxSize = 0;
                break;

            case ALL:
                partitionCacheExpiresAfterWriteMillis = expiresAfterWriteMillis;
                partitionCacheRefreshMills = refreshMills;
                partitionCacheMaxSize = maximumSize;
                cacheExpiresAfterWriteMillis = expiresAfterWriteMillis;
                cacheRefreshMills = refreshMills;
                cacheMaxSize = maximumSize;
                break;

            default:
                throw new IllegalArgumentException("Unknown metastore-cache-scope: " + metastoreCacheScope);
        }

//        databaseNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
//                .build(asyncReloading(CacheLoader.from(this::loadAllDatabases), executor));

        databaseNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllDatabases), executor));

        databaseCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadDatabase), executor));

        tableNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllTables), executor));

        tableStatisticsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(new CacheLoader<KeyAndContext<HiveTableName>, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(KeyAndContext<HiveTableName> key)
                    {
                        return loadTableColumnStatistics(key);
                    }
                }, executor));

        partitionStatisticsCache = newCacheBuilder(partitionCacheExpiresAfterWriteMillis, partitionCacheRefreshMills, partitionCacheMaxSize)
                .build(asyncReloading(new CacheLoader<KeyAndContext<HivePartitionName>, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(KeyAndContext<HivePartitionName> key)
                    {
                        return loadPartitionColumnStatistics(key);
                    }

                    @Override
                    public Map<KeyAndContext<HivePartitionName>, PartitionStatistics> loadAll(Iterable<? extends KeyAndContext<HivePartitionName>> keys)
                    {
                        return loadPartitionColumnStatistics(keys);
                    }
                }, executor));

        tableCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTable), executor));

        viewNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllViews), executor));

        partitionNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNames), executor));

        partitionFilterCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNamesByFilter), executor));

        partitionCache = newCacheBuilder(partitionCacheExpiresAfterWriteMillis, partitionCacheRefreshMills, partitionCacheMaxSize)
                .build(asyncReloading(new CacheLoader<KeyAndContext<HivePartitionName>, Optional<Partition>>()
                {
                    @Override
                    public Optional<Partition> load(KeyAndContext<HivePartitionName> partitionName)
                    {
                        return loadPartitionByName(partitionName);
                    }

                    @Override
                    public Map<KeyAndContext<HivePartitionName>, Optional<Partition>> loadAll(Iterable<? extends KeyAndContext<HivePartitionName>> partitionNames)
                    {
                        return loadPartitionsByNames(partitionNames);
                    }
                }, executor));

        tablePrivilegesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(key -> loadTablePrivileges(key.getDatabase(), key.getTable(), key.getPrincipal())), executor));

        rolesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(() -> loadRoles()), executor));

        roleGrantsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadRoleGrants), executor));
    }

    @Managed
    public void flushCache()
    {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        viewNamesCache.invalidateAll();
        partitionNamesCache.invalidateAll();
        databaseCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        partitionFilterCache.invalidateAll();
        tablePrivilegesCache.invalidateAll();
        tableStatisticsCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        rolesCache.invalidateAll();
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys)
    {
        try {
            return cache.getAll(keys);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        return get(databaseCache, new KeyAndContext<>(metastoreContext, databaseName));
    }

    private Optional<Database> loadDatabase(KeyAndContext<String> databaseName)
    {
        return delegate.getDatabase(databaseName.getMetastoreContext(), databaseName.getKey());
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        metastoreContext = updateIdentity(metastoreContext);
        return get(databaseNamesCache, new KeyAndContext<>(metastoreContext, ""));
    }

    private List<String> loadAllDatabases(KeyAndContext<String> key)
    {
        return delegate.getAllDatabases(key.getMetastoreContext());
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        return get(tableCache, new KeyAndContext<>(metastoreContext, hiveTableName(databaseName, tableName)));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    private Optional<Table> loadTable(KeyAndContext<HiveTableName> hiveTableName)
    {
        return delegate.getTable(hiveTableName.getMetastoreContext(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        return get(tableStatisticsCache, new KeyAndContext<>(metastoreContext, hiveTableName(databaseName, tableName)));
    }

    private PartitionStatistics loadTableColumnStatistics(KeyAndContext<HiveTableName> hiveTableName)
    {
        return delegate.getTableStatistics(hiveTableName.getMetastoreContext(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        List<KeyAndContext<HivePartitionName>> partitions = partitionNames.stream()
                .map(partitionName -> new KeyAndContext<>(updateIdentity(metastoreContext), HivePartitionName.hivePartitionName(databaseName, tableName, partitionName)))
                .collect(toImmutableList());
        Map<KeyAndContext<HivePartitionName>, PartitionStatistics> statistics = getAll(partitionStatisticsCache, partitions);
        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(), Entry::getValue));
    }

    private PartitionStatistics loadPartitionColumnStatistics(KeyAndContext<HivePartitionName> partition)
    {
        String partitionName = partition.getKey().getPartitionName().get();
        Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(
                partition.getMetastoreContext(),
                partition.getKey().getHiveTableName().getDatabaseName(),
                partition.getKey().getHiveTableName().getTableName(),
                ImmutableSet.of(partitionName));
        if (!partitionStatistics.containsKey(partitionName)) {
            throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partition.getKey().getPartitionName());
        }
        return partitionStatistics.get(partitionName);
    }

    private Map<KeyAndContext<HivePartitionName>, PartitionStatistics> loadPartitionColumnStatistics(Iterable<? extends KeyAndContext<HivePartitionName>> keys)
    {
        SetMultimap<KeyAndContext<HiveTableName>, KeyAndContext<HivePartitionName>> tablePartitions = stream(keys)
                .collect(toImmutableSetMultimap(value -> new KeyAndContext<>(value.getMetastoreContext(), value.getKey().getHiveTableName()), key -> key));
        ImmutableMap.Builder<KeyAndContext<HivePartitionName>, PartitionStatistics> result = ImmutableMap.builder();
        tablePartitions.keySet().forEach(table -> {
            Set<String> partitionNames = tablePartitions.get(table).stream()
                    .map(partitionName -> partitionName.getKey().getPartitionName().get())
                    .collect(toImmutableSet());
            Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(table.getMetastoreContext(), table.getKey().getDatabaseName(), table.getKey().getTableName(), partitionNames);
            for (String partitionName : partitionNames) {
                if (!partitionStatistics.containsKey(partitionName)) {
                    throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partitionName);
                }
                result.put(new KeyAndContext<>(table.getMetastoreContext(), hivePartitionName(table.getKey(), partitionName)), partitionStatistics.get(partitionName));
            }
        });
        return result.build();
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.updateTableStatistics(metastoreContext, databaseName, tableName, update);
        }
        finally {
            tableStatisticsCache.invalidate(new KeyAndContext<>(metastoreContext, hiveTableName(databaseName, tableName)));
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.updatePartitionStatistics(metastoreContext, databaseName, tableName, partitionName, update);
        }
        finally {
            partitionStatisticsCache.invalidate(new KeyAndContext<>(metastoreContext, hivePartitionName(databaseName, tableName, partitionName)));
        }
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        return get(tableNamesCache, new KeyAndContext<>(metastoreContext, databaseName));
    }

    private Optional<List<String>> loadAllTables(KeyAndContext<String> databaseName)
    {
        return delegate.getAllTables(databaseName.getMetastoreContext(), databaseName.getKey());
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        return get(viewNamesCache, new KeyAndContext<>(metastoreContext, databaseName));
    }

    private Optional<List<String>> loadAllViews(KeyAndContext<String> databaseName)
    {
        return delegate.getAllViews(databaseName.getMetastoreContext(), databaseName.getKey());
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.createDatabase(metastoreContext, database);
        }
        finally {
            invalidateDatabase(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.dropDatabase(metastoreContext, databaseName);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.renameDatabase(metastoreContext, databaseName, newDatabaseName);
        }
        finally {
            invalidateDatabase(databaseName);
            invalidateDatabase(newDatabaseName);
        }
    }

    protected void invalidateDatabase(String databaseName)
    {
        invalidateDatabaseCache(databaseName);
        invalidateDatabaseNamesCache(databaseName);
    }

    private void invalidateDatabaseCache(String databaseName)
    {
        databaseCache.asMap().keySet().stream()
                .filter(database -> database.getKey().equals(databaseName))
                .forEach(databaseCache::invalidate);
    }

    private void invalidateDatabaseNamesCache(String databaseName)
    {
        databaseNamesCache.asMap().keySet().stream()
                .filter(dbName -> databaseNamesCache.getUnchecked(dbName).contains(databaseName))
                .forEach(databaseNamesCache::invalidate);
    }

    @Override
    public void createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.createTable(metastoreContext, table, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.dropTable(metastoreContext, databaseName, tableName, deleteData);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.replaceTable(metastoreContext, databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public void renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.renameTable(metastoreContext, databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newDatabaseName, newTableName);
        }
    }

    @Override
    public void addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.addColumn(metastoreContext, databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.renameColumn(metastoreContext, databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.dropColumn(metastoreContext, databaseName, tableName, columnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    protected void invalidateTable(String databaseName, String tableName)
    {
        invalidateTableCache(databaseName, tableName);
        invalidateTableNamesCache(databaseName);
        invalidateViewNamesCache(databaseName);
        tablePrivilegesCache.asMap().keySet().stream()
                .filter(userTableKey -> userTableKey.matches(databaseName, tableName))
                .forEach(tablePrivilegesCache::invalidate);
        invalidateTableStatisticsCache(databaseName, tableName);
        invalidatePartitionCache(databaseName, tableName);
    }

    private void invalidateTableCache(String databaseName, String tableName)
    {
        tableCache.asMap().keySet().stream()
                .filter(table -> table.getKey().getDatabaseName().equals(databaseName) && table.getKey().getTableName().equals(tableName))
                .forEach(tableCache::invalidate);
    }

    private void invalidateTableNamesCache(String databaseName)
    {
        tableNamesCache.asMap().keySet().stream()
                .filter(tableName -> tableName.getKey().equals(databaseName))
                .forEach(tableNamesCache::invalidate);
    }

    private void invalidateViewNamesCache(String databaseName)
    {
        viewNamesCache.asMap().keySet().stream()
                .filter(viewName -> viewName.getKey().equals(databaseName))
                .forEach(viewNamesCache::invalidate);
    }

    private void invalidateTableStatisticsCache(String databaseName, String tableName)
    {
        tableStatisticsCache.asMap().keySet().stream()
                .filter(table -> table.getKey().getDatabaseName().equals(databaseName) && table.getKey().getTableName().equals(tableName))
                .forEach(tableStatisticsCache::invalidate);
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        metastoreContext = updateIdentity(metastoreContext);
        KeyAndContext<HivePartitionName> name = new KeyAndContext<>(metastoreContext, hivePartitionName(databaseName, tableName, partitionValues));
        return get(partitionCache, name);
    }

    @Override
    public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        metastoreContext = updateIdentity(metastoreContext);
        return get(partitionNamesCache, new KeyAndContext<>(metastoreContext, hiveTableName(databaseName, tableName)));
    }

    private Optional<List<String>> loadPartitionNames(KeyAndContext<HiveTableName> hiveTableName)
    {
        return delegate.getPartitionNames(hiveTableName.getMetastoreContext(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        if (partitionVersioningEnabled) {
            List<PartitionNameWithVersion> partitionNamesWithVersion = getPartitionNamesWithVersionByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
            List<String> result = partitionNamesWithVersion.stream().map(PartitionNameWithVersion::getPartitionName).collect(toImmutableList());
            partitionNamesWithVersion.forEach(partitionNameWithVersion -> invalidateStalePartition(updateIdentity(metastoreContext), partitionNameWithVersion, databaseName, tableName));
            return result;
        }
        return get(
                partitionFilterCache,
                new KeyAndContext<>(metastoreContext, partitionFilter(databaseName, tableName, partitionPredicates)));
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesWithVersionByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
    }

    private void invalidateStalePartition(MetastoreContext metastoreContext, PartitionNameWithVersion partitionNameWithVersion, String databaseName, String tableName)
    {
        KeyAndContext<HivePartitionName> hivePartitionName = new KeyAndContext<>(metastoreContext, hivePartitionName(databaseName, tableName, partitionNameWithVersion.getPartitionName()));
        Optional<Partition> partition = partitionCache.getIfPresent(hivePartitionName);
        if (partition != null && partition.isPresent()) {
            Optional<Long> partitionVersion = partition.get().getPartitionVersion();
            if (!partitionVersion.isPresent() || partitionVersion.get() != partitionNameWithVersion.getPartitionVersion()) {
                partitionCache.invalidate(hivePartitionName);
                partitionStatisticsCache.invalidate(hivePartitionName);
            }
        }
    }

    private List<String> loadPartitionNamesByFilter(KeyAndContext<PartitionFilter> partitionFilter)
    {
        return delegate.getPartitionNamesByFilter(
                partitionFilter.getMetastoreContext(),
                partitionFilter.getKey().getHiveTableName().getDatabaseName(),
                partitionFilter.getKey().getHiveTableName().getTableName(),
                partitionFilter.getKey().getPartitionPredicates());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        Iterable<KeyAndContext<HivePartitionName>> names = transform(partitionNames, name -> new KeyAndContext<>(updateIdentity(metastoreContext), hivePartitionName(databaseName, tableName, name)));

        Map<KeyAndContext<HivePartitionName>, Optional<Partition>> all = getAll(partitionCache, names);
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<KeyAndContext<HivePartitionName>, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getKey().getPartitionName().get(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Optional<Partition> loadPartitionByName(KeyAndContext<HivePartitionName> partitionName)
    {
        return delegate.getPartition(
                partitionName.getMetastoreContext(),
                partitionName.getKey().getHiveTableName().getDatabaseName(),
                partitionName.getKey().getHiveTableName().getTableName(),
                partitionName.getKey().getPartitionValues());
    }

    private Map<KeyAndContext<HivePartitionName>, Optional<Partition>> loadPartitionsByNames(Iterable<? extends KeyAndContext<HivePartitionName>> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        KeyAndContext<HivePartitionName> firstPartition = Iterables.get(partitionNames, 0);

        HiveTableName hiveTableName = firstPartition.getKey().getHiveTableName();
        MetastoreContext metastoreContext = updateIdentity(firstPartition.getMetastoreContext());
        String databaseName = hiveTableName.getDatabaseName();
        String tableName = hiveTableName.getTableName();

        List<String> partitionsToFetch = new ArrayList<>();
        for (KeyAndContext<HivePartitionName> partitionName : partitionNames) {
            checkArgument(partitionName.getKey().getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionName.getKey().getHiveTableName());
            partitionsToFetch.add(partitionName.getKey().getPartitionName().get());
        }

        ImmutableMap.Builder<KeyAndContext<HivePartitionName>, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(metastoreContext, databaseName, tableName, partitionsToFetch);
        for (Entry<String, Optional<Partition>> entry : partitionsByNames.entrySet()) {
            partitions.put(new KeyAndContext<>(metastoreContext, hivePartitionName(hiveTableName, entry.getKey())), entry.getValue());
        }
        return partitions.build();
    }

    @Override
    public void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.addPartitions(metastoreContext, databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.dropPartition(metastoreContext, databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        metastoreContext = updateIdentity(metastoreContext);
        try {
            delegate.alterPartition(metastoreContext, databaseName, tableName, partition);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        try {
            delegate.createRole(role, grantor);
        }
        finally {
            rolesCache.invalidateAll();
        }
    }

    @Override
    public void dropRole(String role)
    {
        try {
            delegate.dropRole(role);
        }
        finally {
            rolesCache.invalidateAll();
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<String> listRoles()
    {
        return get(rolesCache, "");
    }

    private Set<String> loadRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        try {
            delegate.grantRoles(roles, grantees, withAdminOption, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        try {
            delegate.revokeRoles(roles, grantees, adminOptionFor, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(PrestoPrincipal principal)
    {
        return get(roleGrantsCache, principal);
    }

    private Set<RoleGrant> loadRoleGrants(PrestoPrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    private void invalidatePartitionCache(String databaseName, String tableName)
    {
        HiveTableName hiveTableName = hiveTableName(databaseName, tableName);
        partitionNamesCache.asMap().keySet().stream()
                .filter(partitionName -> partitionName.getKey().equals(hiveTableName))
                .forEach(partitionNamesCache::invalidate);
        partitionCache.asMap().keySet().stream()
                .filter(partitionName -> partitionName.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionCache::invalidate);
        partitionFilterCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionFilterCache::invalidate);
        partitionStatisticsCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionStatisticsCache::invalidate);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(grantee, databaseName, tableName));
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(grantee, databaseName, tableName));
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        return get(tablePrivilegesCache, new UserTableKey(principal, databaseName, tableName));
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return delegate.isImpersonationEnabled();
    }

    public Set<HivePrivilegeInfo> loadTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, principal);
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(OptionalLong expiresAfterWriteMillis, OptionalLong refreshMillis, long maximumSize)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        if (refreshMillis.isPresent() && (!expiresAfterWriteMillis.isPresent() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheBuilder = cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
        }
        cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private static class KeyAndContext<T>
    {
        private final MetastoreContext metastoreContext;
        private final T key;

        public KeyAndContext(MetastoreContext metastoreContext, T key)
        {
            this.metastoreContext = requireNonNull(metastoreContext, "metastoreContext is null");
            this.key = requireNonNull(key, "key is null");
        }

        public MetastoreContext getMetastoreContext()
        {
            return metastoreContext;
        }

        public T getKey()
        {
            return key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyAndContext<?> other = (KeyAndContext<?>) o;
            return Objects.equals(metastoreContext, other.metastoreContext) &&
                    Objects.equals(key, other.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(metastoreContext, key);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("metastoreContext", metastoreContext)
                    .add("key", key)
                    .toString();
        }
    }

    private MetastoreContext updateIdentity(MetastoreContext metastoreContext)
    {
        // remove metastoreContext if not doing impersonation
        return delegate.isImpersonationEnabled() ? metastoreContext : MetastoreContext.none();
    }
}
