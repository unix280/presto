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
package com.facebook.presto.hive.security.ranger;

import com.amazonaws.util.StringUtils;
import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.Set;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RangerAuthorizer
{
    private static final Logger LOG = Logger.get(RangerAuthorizer.class);
    private static volatile RangerBasePlugin plugin;
    public static final String CLUSTER_NAME = "Presto";
    public static final String HIVE = "hive";

    public RangerAuthorizer(ServicePolicies servicePolicies, RangerBasedAccessControlConfig rangerBasedAccessControlConfig)
    {
        RangerPolicyEngineOptions rangerPolicyEngineOptions = new RangerPolicyEngineOptions();
        Configuration conf = new Configuration();
        rangerPolicyEngineOptions.configureDefaultRangerAdmin(conf, "hive");
        RangerPluginConfig rangerPluginConfig = new RangerPluginConfig(HIVE, rangerBasedAccessControlConfig.getRangerHiveServiceName(), HIVE, CLUSTER_NAME, null,
                rangerPolicyEngineOptions);
        plugin = new RangerBasePlugin(rangerPluginConfig);

        String hiveAuditPath = rangerBasedAccessControlConfig.getRangerHiveAuditPath();
        if (!StringUtils.isNullOrEmpty(hiveAuditPath)) {
            try {
                plugin.getConfig().addResource(new File(hiveAuditPath).toURI().toURL());
            }
            catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        AuditProviderFactory providerFactory = AuditProviderFactory.getInstance();

        if (!providerFactory.isInitDone()) {
            if (plugin.getConfig().getProperties() != null) {
                providerFactory.init(plugin.getConfig().getProperties(), HIVE);
            }
            else {
                LOG.error("Audit subsystem is not initialized correctly. Please check audit configuration. ");
                LOG.error("No authorization audits will be generated. ");
            }
        }

        plugin.setResultProcessor(new RangerDefaultAuditHandler());
        plugin.setPolicies(servicePolicies);
    }

    private static <T> T jsonParse(Response response, Class<T> clazz)
            throws IOException
    {
        BufferedReader bufferedReader = null;
        if (response.body() != null) {
            bufferedReader = new BufferedReader(new InputStreamReader(response.body().byteStream()));
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bufferedReader, clazz);
    }

    public static Interceptor basicAuth(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");

        String credential = Credentials.basic(user, password);
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build());
    }

    private static OkHttpClient getOkHttpAuthClient(String username, String password)
    {
        OkHttpClient httpClient = new OkHttpClient.Builder().build();
        OkHttpClient.Builder builder = httpClient.newBuilder();
        builder.addInterceptor(basicAuth(username, password));
        return builder.build();
    }

    public boolean authorizeHiveResource(String database, String table, String column, String accessType, String user, Set<String> userGroups, Set<String> userRoles)
    {
        String keyDatabase = "database";
        String keyTable = "table";
        String keyColumn = "column";
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        if (!StringUtils.isNullOrEmpty(database)) {
            resource.setValue(keyDatabase, database);
        }

        if (!StringUtils.isNullOrEmpty(table)) {
            resource.setValue(keyTable, table);
        }

        if (!StringUtils.isNullOrEmpty(column)) {
            resource.setValue(keyColumn, column);
        }

        RangerAccessRequest request = new RangerAccessRequestImpl(resource, accessType.toLowerCase(ENGLISH), user, userGroups, userRoles);
        RangerAccessResult result = plugin.isAccessAllowed(request);
        return result != null && result.getIsAllowed();
    }

    private static Response doRequest(OkHttpClient httpClient, String anyURL)
            throws IOException
    {
        Request request = new Request.Builder().url(anyURL).header("Accept", "application/json").build();
        Response response = httpClient.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }
        return response;
    }
}
