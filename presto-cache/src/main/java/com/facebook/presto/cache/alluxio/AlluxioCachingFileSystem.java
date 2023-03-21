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
package com.facebook.presto.cache.alluxio;

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.hadoop.LocalCacheFileSystem;
import alluxio.metrics.MetricsSystem;
import alluxio.shaded.client.com.codahale.metrics.Meter;
import alluxio.wire.FileInfo;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static alluxio.Constants.SCHEME;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED;
import static alluxio.metrics.MetricKey.CLIENT_CACHE_BYTES_READ_CACHE;
import static alluxio.metrics.MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL;
import static alluxio.metrics.MetricKey.CLIENT_CACHE_BYTES_WRITTEN_CACHE;
import static alluxio.metrics.MetricKey.CLIENT_CACHE_HIT_RATE;
import static alluxio.metrics.MetricKey.CLIENT_CACHE_PAGES_EVICTED;
import static alluxio.metrics.MetricsSystem.getMetricName;
import static alluxio.metrics.MetricsSystem.meter;
import static alluxio.metrics.MetricsSystem.registerGaugeIfAbsent;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AlluxioCachingFileSystem
        extends CachingFileSystem
{
    private static final int BUFFER_SIZE = 65536;
    private final boolean cacheValidationEnabled;
    private boolean cacheQuotaEnabled;
    private LocalCacheFileSystem localCacheFileSystem;

    public AlluxioCachingFileSystem(ExtendedFileSystem dataTier, URI uri)
    {
        this(dataTier, uri, false);
    }

    public AlluxioCachingFileSystem(ExtendedFileSystem dataTier, URI uri, boolean cacheValidationEnabled)
    {
        super(dataTier, uri);
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration configuration)
            throws IOException
    {
        this.localCacheFileSystem = new LocalCacheFileSystem(dataTier, uriStatus -> {
            // CacheContext is the mechanism to pass the hiveFileContext to the source filesystem
            // hiveFileContext is critical to use to open file.
            CacheContext cacheContext = uriStatus.getCacheContext();
            checkState(cacheContext instanceof PrestoCacheContext);
            HiveFileContext hiveFileContext = ((PrestoCacheContext) cacheContext).getHiveFileContext();
            try {
                return dataTier.openFile(new Path(uriStatus.getPath()), hiveFileContext);
            }
            catch (Exception e) {
                throw new IOException("Failed to open file", e);
            }
        });

        // create an URI with alluxio:// scheme for use with Alluxio
        URI alluxioUri;
        try {
            alluxioUri = new URI(SCHEME, uri.getSchemeSpecificPart(), uri.getFragment());
        }
        catch (URISyntaxException e) {
            throw new IOException("Unable to create Alluxio URI", e);
        }

        this.cacheQuotaEnabled = configuration.getBoolean(USER_CLIENT_CACHE_QUOTA_ENABLED.getName(), false);
        localCacheFileSystem.initialize(alluxioUri, configuration);
        // Some of the local cache read metrics like bytes read, hit rate etc are registered
        // only when a LocalCacheFileInStream object is created as part of openFile method below.
        // In Presto, when the coordinator is not acting as a worker, file read might only happen
        // on workers, while metrics are only exposed from coordinators, resulting in the metrics
        // not being registered. Always register the metrics when initializing this class.
        // A fix https://github.com/Alluxio/alluxio/pull/14912 for this in alluxio project is also
        // in progress. Once we upgrade to alluxio version with this fix, we can safely remove
        // the below call and associated changes.
        Metrics.registerGauges();
        Metrics.registerMeters();
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        // Using Alluxio caching requires knowing file size for now
        if (hiveFileContext.isCacheable() && hiveFileContext.getFileSize().isPresent()) {
            // FilePath is a unique identifier for a file, however it can be a long string
            // hence using md5 hash of the file path as the identifier in the cache.
            // We don't set fileId because fileId is Alluxio specific
            FileInfo info = new FileInfo()
                    .setLastModificationTimeMs(hiveFileContext.getModificationTime())
                    .setPath(path.toString())
                    .setFolder(false)
                    .setLength(hiveFileContext.getFileSize().getAsLong());
            String cacheIdentifier = md5().hashString(path.toString(), UTF_8).toString();
            // CacheContext is the mechanism to pass the cache related context to the source filesystem
            CacheContext cacheContext = PrestoCacheContext.build(cacheIdentifier, hiveFileContext, cacheQuotaEnabled);
            URIStatus uriStatus = new URIStatus(info, cacheContext);
            FSDataInputStream cachingInputStream = localCacheFileSystem.open(uriStatus, BUFFER_SIZE);
            if (cacheValidationEnabled) {
                return new CacheValidatingInputStream(
                        cachingInputStream, dataTier.openFile(path, hiveFileContext));
            }
            return cachingInputStream;
        }
        return dataTier.openFile(path, hiveFileContext);
    }

    private static final class Metrics
    {
        /** Cache hits. */
        private static final Meter BYTES_READ_CACHE =
                meter(CLIENT_CACHE_BYTES_READ_CACHE.getName());
        /** Cache misses. */
        private static final Meter BYTES_REQUESTED_EXTERNAL =
                meter(CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName());

        private static void registerGauges()
        {
            // Cache hit rate = Cache hits / (Cache hits + Cache misses).
            registerGaugeIfAbsent(
                    getMetricName(CLIENT_CACHE_HIT_RATE.getName()),
                    () -> {
                        long cacheHits = BYTES_READ_CACHE.getCount();
                        long cacheMisses = BYTES_REQUESTED_EXTERNAL.getCount();
                        long total = cacheHits + cacheMisses;
                        if (total > 0) {
                            return cacheHits / (1.0 * total);
                        }
                        return 0;
                    });
        }

        private static void registerMeters()
        {
            // In recent changes of alluxio, the below metrics were inlined, so unless there is a cache
            // eviction/cache write during a query run, it was not being registered. Even then, it won't
            // be registered on the coordinator and jmx metrics won't show these. To initialize these on
            // the coordinator on startup rather than on any cache operations, adding it here.
            MetricsSystem.meter(CLIENT_CACHE_PAGES_EVICTED.getName());
            MetricsSystem.meter(CLIENT_CACHE_BYTES_WRITTEN_CACHE.getName());
        }
    }
}
