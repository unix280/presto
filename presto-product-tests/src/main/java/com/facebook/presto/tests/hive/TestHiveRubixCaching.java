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
package com.facebook.presto.tests.hive;

import io.airlift.units.Duration;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.testing.assertions.Assert.assertEventually;
import static com.facebook.presto.tests.TestGroups.HIVE_CACHING;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestHiveRubixCaching
        extends ProductTest
{
    @Test(groups = {HIVE_CACHING, PROFILE_SPECIFIC_TESTS})
    public void testReadFromCache()
    {
        String cachedTableName = "hive.default.test_cache_read";
        String nonCachedTableName = "hivenoncached.default.test_cache_read";

        query("DROP TABLE IF EXISTS " + nonCachedTableName);
        query("CREATE TABLE " + nonCachedTableName + " WITH (format='ORC') AS SELECT 'Hello world' as col");

        QueryResult beforeCacheStats = getCacheStats();
        long initialRemoteReads = getRemoteReads(beforeCacheStats);
        long initialCachedReads = getCachedReads(beforeCacheStats);

        assertThat(query("SELECT * FROM " + cachedTableName))
                .containsExactly(row("Hello world"));

        assertEventually(
                new Duration(20, SECONDS),
                () -> {
                    // first query via caching catalog should fetch remote data
                    QueryResult afterQueryCacheStats = getCacheStats();
                    assertGreaterThan(getRemoteReads(afterQueryCacheStats), initialRemoteReads);
                    assertEquals(getCachedReads(afterQueryCacheStats), initialCachedReads);
                });

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    QueryResult beforeQueryCacheStats = getCacheStats();
                    long beforeQueryCachedReads = getCachedReads(beforeQueryCacheStats);
                    long beforeQueryRemoteReads = getRemoteReads(beforeQueryCacheStats);

                    assertThat(query("SELECT * FROM " + cachedTableName))
                            .containsExactly(row("Hello world"));

                    // query via caching catalog should read exclusively from cache
                    QueryResult afterQueryCacheStats = getCacheStats();
                    assertGreaterThan(getCachedReads(afterQueryCacheStats), beforeQueryCachedReads);
                    assertEquals(getRemoteReads(afterQueryCacheStats), beforeQueryRemoteReads);
                });

        query("DROP TABLE " + nonCachedTableName);
    }

    private QueryResult getCacheStats()
    {
        return query("SELECT " +
                "  sum(Cached_rrc_requests) as cachedreads, " +
                "  sum(Remote_rrc_requests + Direct_rrc_requests) as remotereads " +
                "FROM jmx.current.\"rubix:catalog=hive,type=detailed,name=stats\";");
    }

    private long getCachedReads(QueryResult queryResult)
    {
        return (Long) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("cachedreads").get() - 1);
    }

    private long getRemoteReads(QueryResult queryResult)
    {
        return (Long) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("remotereads").get() - 1);
    }
}
