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
package org.apache.hadoop.fs;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hadoop.FileSystemFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FileSystem.getFileSystemClass;
import static org.apache.hadoop.security.UserGroupInformationShim.getSubject;

public class PrestoFileSystemCache
        extends FileSystem.Cache
{
    private static final Logger log = Logger.get(PrestoFileSystemCache.class);

    public static final String PRESTO_GCS_OAUTH_ACCESS_TOKEN_KEY = "presto.gcs.oauth-access-token";
    public static final String PRESTO_S3_IAM_ROLE = "presto.hive.s3.iam-role";
    public static final String PRESTO_S3_ACCESS_KEY = "presto.s3.access-key";
    public static final String PRESTO_S3_LAKE_FORMATION_CACHE_KEY = "presto.s3.lake-formation-cache-key";

    private final AtomicLong unique = new AtomicLong();
    private final Map<FileSystemKey, FileSystemHolder> cache = new ConcurrentHashMap<>();
    /*
     * ConcurrentHashMap has a lock per partitioned key-space bucket, and hence there is no consistent
     * or 'serialized' view of current number of entries in the map from a thread that would like to
     * add/delete/update an entry. As we have to limit the max size of the cache to 'fs.cache.max-size',
     * an auxiliary variable `cacheSize` is used to track the 'serialized' view of entry count in the cache.
     * cacheSize should only be updated after acquiring cache's partition lock (eg: from inside cache.compute())
     */
    private final AtomicLong cacheSize = new AtomicLong();

    public PrestoFileSystemCache() {}

    @Override
    public FileSystem get(URI uri, Configuration conf)
            throws IOException
    {
        if (conf instanceof FileSystemFactory) {
            return ((FileSystemFactory) conf).createFileSystem(uri);
        }

        return getInternal(uri, conf, 0);
    }

    @Override
    FileSystem getUnique(URI uri, Configuration conf)
            throws IOException
    {
        if (conf instanceof FileSystemFactory) {
            return ((FileSystemFactory) conf).createFileSystem(uri);
        }

        return getInternal(uri, conf, unique.incrementAndGet());
    }

    private FileSystem getInternal(URI uri, Configuration conf, long unique)
            throws IOException
    {
        UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();
        FileSystemKey key = createFileSystemKey(uri, userGroupInformation, conf, unique);
        Set<?> privateCredentials = getPrivateCredentials(userGroupInformation);

        int maxSize = conf.getInt("fs.cache.max-size", 1000);
        FileSystemHolder fileSystemHolder;
        try {
            fileSystemHolder = cache.compute(key, (k, currentFileSystemHolder) -> {
                if (currentFileSystemHolder == null) {
                    if (cacheSize.getAndUpdate(currentSize -> Math.min(currentSize + 1, maxSize)) >= maxSize) {
                        throw new RuntimeException(
                                new IOException(format("FileSystem max cache size has been reached: %s", maxSize)));
                    }
                    return new FileSystemHolder(conf, privateCredentials);
                }
                else {
                    // Update file system instance when credentials change
                    // Private credentials are only set when using Kerberos authentication.
                    // When the user is the same, but the private credentials are different,
                    // that means that Kerberos ticket has expired and re-login happened.
                    // To prevent cache leak in such situation, the privateCredentials are not
                    // a part of the FileSystemKey, but part of the FileSystemHolder. When a
                    // Kerberos re-login occurs, re-create the file system and cache it using
                    // the same key.
                    if (currentFileSystemHolder.fileSystemHolderRefresh(uri, conf, privateCredentials)) {
                        return new FileSystemHolder(conf, privateCredentials);
                    }
                    else {
                        return currentFileSystemHolder;
                    }
                }
            });

            // Now create the filesystem object outside of cache's lock
            fileSystemHolder.createFileSystemOnce(uri, conf);
        }
        catch (RuntimeException | IOException e) {
            throwIfInstanceOf(e, IOException.class);
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw e;
        }

        return fileSystemHolder.getFileSystem();
    }

    private static FileSystem createFileSystem(URI uri, Configuration conf)
            throws IOException
    {
        Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);
        if (clazz == null) {
            throw new IOException("No FileSystem for scheme: " + uri.getScheme());
        }
        FileSystem original = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
        original.initialize(uri, conf);
        FileSystem wrapper = createPrestoFileSystemWrapper(original);
        FileSystemFinalizerService.getInstance().addFinalizer(wrapper, () -> {
            try {
                original.close();
            }
            catch (IOException e) {
                log.error("Error occurred when finalizing file system", e);
            }
        });
        return wrapper;
    }

    protected static FileSystem createPrestoFileSystemWrapper(FileSystem original)
    {
        return new HadoopExtendedFileSystem(original);
    }

    @Override
    public void remove(Key ignored, FileSystem fileSystem)
    {
        if (fileSystem == null) {
            return;
        }
        cache.forEach((fileSystemKey, fileSystemHolder) -> {
            if (fileSystem.equals(fileSystemHolder.getFileSystem())) {
                // After acquiring the lock, decrement cacheSize only if
                // (1) the key is still mapped to a FileSystemHolder
                // (2) the filesystem object inside FileSystemHolder is the same
                cache.compute(fileSystemKey, (k, currentFileSystemHolder) -> {
                    if (currentFileSystemHolder != null
                            && fileSystem.equals(currentFileSystemHolder.getFileSystem())) {
                        cacheSize.decrementAndGet();
                        return null;
                    }
                    return currentFileSystemHolder;
                });
            }
        });
    }

    @Override
    void closeAll()
            throws IOException
    {
        try {
            cache.forEach((key, fileSystemHolder) -> {
                try {
                    cache.compute(key, (k, currentFileSystemHolder) -> {
                        // decrement cacheSize only if the key is still mapped
                        if (currentFileSystemHolder != null) {
                            cacheSize.decrementAndGet();
                        }
                        return null;
                    });
                    FileSystem fs = fileSystemHolder.getFileSystem();
                    if (fs != null) {
                        fs.close();
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (RuntimeException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw e;
        }
    }

    @Override
    synchronized void closeAll(boolean onlyAutomatic)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    synchronized void closeAll(UserGroupInformation ugi)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    private static FileSystemKey createFileSystemKey(URI uri, UserGroupInformation userGroupInformation, Configuration configuration, long unique)
    {
        String scheme = nullToEmpty(uri.getScheme()).toLowerCase(ENGLISH);
        String authority = nullToEmpty(uri.getAuthority()).toLowerCase(ENGLISH);
        String realUser;
        String proxyUser;
        UserGroupInformation.AuthenticationMethod authenticationMethod = userGroupInformation.getAuthenticationMethod();
        String extraInfo = nullToEmpty(configuration.get(PRESTO_S3_LAKE_FORMATION_CACHE_KEY)).toLowerCase(ENGLISH);
        switch (authenticationMethod) {
            case SIMPLE:
            case KERBEROS:
                realUser = userGroupInformation.getUserName();
                proxyUser = null;
                break;
            case PROXY:
                realUser = userGroupInformation.getRealUser().getUserName();
                proxyUser = userGroupInformation.getUserName();
                break;
            default:
                throw new IllegalArgumentException("Unsupported authentication method: " + authenticationMethod);
        }
        return new FileSystemKey(scheme, authority, unique, realUser, proxyUser, extraInfo);
    }

    private static Set<?> getPrivateCredentials(UserGroupInformation userGroupInformation)
    {
        UserGroupInformation.AuthenticationMethod authenticationMethod = userGroupInformation.getAuthenticationMethod();
        switch (authenticationMethod) {
            case SIMPLE:
                return ImmutableSet.of();
            case KERBEROS:
                return ImmutableSet.copyOf(getSubject(userGroupInformation).getPrivateCredentials());
            case PROXY:
                return getPrivateCredentials(userGroupInformation.getRealUser());
            default:
                throw new IllegalArgumentException("Unsupported authentication method: " + authenticationMethod);
        }
    }

    private static boolean isHdfs(URI uri)
    {
        String scheme = uri.getScheme();
        return "hdfs".equals(scheme) || "viewfs".equals(scheme);
    }

    private static class FileSystemKey
    {
        private final String scheme;
        private final String authority;
        private final long unique;
        private final String realUser;
        private final String proxyUser;
        private final String extraInfo;

        public FileSystemKey(String scheme, String authority, long unique, String realUser, String proxyUser, String extraInfo)
        {
            this.scheme = requireNonNull(scheme, "scheme is null");
            this.authority = requireNonNull(authority, "authority is null");
            this.unique = unique;
            this.realUser = requireNonNull(realUser, "realUser");
            this.proxyUser = proxyUser;
            this.extraInfo = extraInfo;
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
            FileSystemKey that = (FileSystemKey) o;
            return Objects.equals(scheme, that.scheme) &&
                    Objects.equals(authority, that.authority) &&
                    Objects.equals(unique, that.unique) &&
                    Objects.equals(realUser, that.realUser) &&
                    Objects.equals(proxyUser, that.proxyUser) &&
                    Objects.equals(extraInfo, that.extraInfo);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(scheme, authority, unique, realUser, proxyUser, extraInfo);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("scheme", scheme)
                    .add("authority", authority)
                    .add("unique", unique)
                    .add("realUser", realUser)
                    .add("proxyUser", proxyUser)
                    .add("extraInfo", extraInfo)
                    .toString();
        }
    }

    private static class FileSystemHolder
    {
        public static final String GOOGLE_CLOUD_STORAGE_FILE_SYSTEM_SCHEME = "gs";
        public static final String AWS_S3_FILE_SYSTEM_SCHEME = "s3";
        private final Set<?> privateCredentials;
        private final String gcsToken;
        private final String awsAccessKey;
        private final String awsIAMRole;
        private volatile FileSystem fileSystem;

        public FileSystemHolder(Configuration conf, Set<?> privateCredentials)
        {
            this.privateCredentials = ImmutableSet.copyOf(requireNonNull(privateCredentials, "privateCredentials is null"));
            this.gcsToken = conf.get(PRESTO_GCS_OAUTH_ACCESS_TOKEN_KEY);
            this.awsAccessKey = conf.get(PRESTO_S3_ACCESS_KEY);
            this.awsIAMRole = conf.get(PRESTO_S3_IAM_ROLE);
        }

        public void createFileSystemOnce(URI uri, Configuration conf)
                throws IOException
        {
            if (fileSystem == null) {
                synchronized (this) {
                    if (fileSystem == null) {
                        fileSystem = PrestoFileSystemCache.createFileSystem(uri, conf);
                    }
                }
            }
        }

        public boolean fileSystemHolderRefresh(URI uri, Configuration newConfiguration, Set<?> newPrivateCredentials)
        {
            if (isHdfs(uri)) {
                return !this.privateCredentials.equals(newPrivateCredentials);
            }
            if (GOOGLE_CLOUD_STORAGE_FILE_SYSTEM_SCHEME.equals(uri.getScheme())) {
                String newGcsToken = newConfiguration.get(PRESTO_GCS_OAUTH_ACCESS_TOKEN_KEY);
                if (gcsToken == null) {
                    return newGcsToken != null;
                }
                return !gcsToken.equals(newGcsToken);
            }
            if (uri.getScheme().startsWith(AWS_S3_FILE_SYSTEM_SCHEME)) {
                String newIAMRole = newConfiguration.get(PRESTO_S3_IAM_ROLE);
                String newAccessKey = newConfiguration.get(PRESTO_S3_ACCESS_KEY);

                if (awsAccessKey == null && awsIAMRole == null) {
                    return newIAMRole != null || newAccessKey != null;
                }
                if (awsIAMRole != null) {
                    return !awsIAMRole.equals(newIAMRole);
                }
                return !awsAccessKey.equals(newAccessKey);
            }
            return false;
        }

        public FileSystem getFileSystem()
        {
            return fileSystem;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("fileSystem", fileSystem)
                    .add("privateCredentials", privateCredentials)
                    .add("gcsToken", gcsToken)
                    .add("awsAccessKey", awsAccessKey)
                    .add("awsIAMRole", awsIAMRole)
                    .toString();
        }
    }
}
