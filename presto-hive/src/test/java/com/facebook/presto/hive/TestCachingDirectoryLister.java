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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.TestBackgroundHiveSplitLoader.TestingHdfsFileSystem;
import static com.facebook.presto.hive.TestBackgroundHiveSplitLoader.locatedFileStatus;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static org.testng.Assert.assertEquals;

public class TestCachingDirectoryLister
{
    private static final String SAMPLE_PATH = "hdfs://VOL1:9000/db_name/table_name/000000_0";
    private static final String SAMPLE_PATH_FILTERED = "hdfs://VOL1:9000/db_name/table_name/000000_1";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);

    private static final List<LocatedFileStatus> TEST_FILES = ImmutableList.of(
            locatedFileStatus(RETURNED_PATH, 0L),
            locatedFileStatus(FILTERED_PATH, 0L));

    private static final String SCHEMA_NAME = "test_dbname";
    private static final String TABLE_NAME = "test_table";
    private static final String USER_NAME = "user";
    private static final String LOCATION = "hdfs://VOL1:9000/db_name/table_name";

    private static final Table TABLE = new Table(
            SCHEMA_NAME,
            TABLE_NAME,
            USER_NAME,
            MANAGED_TABLE,
            new Storage(fromHiveStorageFormat(ORC),
                    LOCATION,
                    Optional.empty(),
            false,
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty());

    @Test
    void testCachingDirectoryLister()
    {
        CachingDirectoryLister cachingDirectoryLister = new CachingDirectoryLister(
                new HadoopDirectoryLister(),
                new Duration(5, TimeUnit.MINUTES),
                1000,
                ImmutableList.of("test_dbname.test_table"));
        TestingHdfsFileSystem testingHdfsFileSystem = new TestingHdfsFileSystem(TEST_FILES);
        HiveDirectoryContext cachedHiveDirectoryContext = new HiveDirectoryContext(IGNORED, true);
        // Initial file count retrieved will be 2 and these 2 will file statuses will be cached
        assertEquals(getFileCount(cachedHiveDirectoryContext, testingHdfsFileSystem, cachingDirectoryLister), 2);
        // remove one file
        testingHdfsFileSystem.delete(RETURNED_PATH, false);
        // Since the cache already has files, and we didn't invalidate it after deleting the file above,
        // it will still return count 2
        assertEquals(getFileCount(cachedHiveDirectoryContext, testingHdfsFileSystem, cachingDirectoryLister), 2);
        // Test with file list cache disabled, it will now return the correct value of 1, since it will
        // read the file status from the filesystem again instead of cache.
        assertEquals(getFileCount(new HiveDirectoryContext(IGNORED, false), testingHdfsFileSystem, cachingDirectoryLister), 1);
    }

    private int getFileCount(HiveDirectoryContext hiveDirectoryContext, TestingHdfsFileSystem testingHdfsFileSystem, CachingDirectoryLister cachingDirectoryLister)
    {
        Iterator<HiveFileInfo> fileInfoIterator = cachingDirectoryLister.list(
                testingHdfsFileSystem,
                TABLE,
                new Path(LOCATION),
                new NamenodeStats(),
                hiveDirectoryContext);

        int count = 0;
        while (fileInfoIterator.hasNext()) {
            fileInfoIterator.next();
            count++;
        }

        return count;
    }
}
