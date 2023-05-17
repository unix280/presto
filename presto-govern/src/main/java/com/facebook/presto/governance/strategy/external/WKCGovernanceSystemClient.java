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
package com.facebook.presto.governance.strategy.external;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.governance.entity.GovernPolicyFormat;
import com.facebook.presto.governance.entity.QueryBasicInfo;
import com.facebook.presto.governance.entity.TableBasicInfo;
import com.facebook.presto.governance.entity.UserDefinedFunctionInfo;
import com.facebook.presto.governance.util.StringHelperUtil;
import com.facebook.presto.spi.governance.QureyGovernanceException;
import com.facebook.presto.spi.security.Identity;
import com.ibm.dp.ltsmasking.LtsConvertor;
import com.ibm.dp.ltsmasking.MaskSpecification;
import com.ibm.wdp.policy.pep.PEP;
import com.ibm.wdp.policy.pep.PepConfiguration;
import com.ibm.wdp.policy.pep.PepContext;
import com.ibm.wdp.policy.pep.PepEvaluation;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonValue;
import javax.json.stream.JsonParsingException;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static com.facebook.presto.util.GovernPropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class WKCGovernanceSystemClient
        implements ExternalGovernanceSystemClient
{
    public static final String NAME = "wkc";
    public static final String ESCAPE_DOUBLE_QUOTES = "\"";
    public static final String EMPTY_STRING = "";
    public static final String COMMA_SYMBOL = ",";
    public static final String SINGLE_QUOTE_SYMBOL = "'";
    public static final String TOKEN = "token";
    public static final String PIPE_SYMBOL = "|";
    public static final String MASK_METHOD_NAME = "thinkdemomask";
    public static final String TRANSFORM = "Transform";
    private static final File WKC_CONFIG_PROPERTIES = new File("etc/govern/external/wkc/wkc-lts-config.properties");
    private static final String WKC_ENV_URL_PROPERTY_NAME = "wkc-env.url";
    private static final String LH_HOST_IP_ADDRESS_PROPERTY_NAME = "lh-host.ip";
    private static final String LH_HOST_PORT_PROPERTY_NAME = "lh-host.port";
    private static final String WKC_ENV_USR_NAME_PROPERTY_NAME = "wkc-env.username";
    private static final String WKC_ENV_USR_PASSWORD_PROPERTY_NAME = "wkc-env.password";
    private static final String WKC_QUERY_CONTEXT_CONFIG_USER_PROPERTY_NAME = "wkc-query-context.config.user";

    private static final String WKC_QUERY_CONTEXT_CONFIG_USER_APPLICABLE = "wkc-query-context.config.user.applicable";

    private static final String LTS_MASKING_FILE_PROPERTY_NAME = "lts-masking-filename.path";
    public static final String TRANSFORMS = "transforms";
    public static final String OPERATION_TYPE = "operation_type";
    public static final String DESCRIPTION_TRANSFORM_METADATA = "description/transform_metadata";
    public static final String PARAMETERS = "parameters";
    public static final String SCHEMA = "schema";
    public static final String NAME_VALUE = "name";
    public static final String DATE_DATATYPE = "Date";
    Logger log = Logger.get(WKCGovernanceSystemClient.class);

    static {
        //System.setProperty("lts-masking-filename", "/Users/bentonyjoe/MyWorks/lakehouse/tmp/frommario/liblts-masking.dylib");
        System.setProperty("lts-masking-filename", getPropertyValueFromWKCConfiguration(LTS_MASKING_FILE_PROPERTY_NAME));
    }

    public List<GovernPolicyFormat> fetchrules(QueryBasicInfo queryBasicInfo, String catalog, String schema, Identity identity)
            throws QureyGovernanceException
    {
        List<GovernPolicyFormat> policyformatList = new ArrayList<>();
        if (null != queryBasicInfo && null != queryBasicInfo.getTableBasicInfo() && !queryBasicInfo.getTableBasicInfo().isEmpty()) {
            for (Map.Entry<String, TableBasicInfo> entry : queryBasicInfo.getTableBasicInfo().entrySet()) {
                GovernPolicyFormat policyFormatObj = new GovernPolicyFormat();
                TableBasicInfo tableInfoObj = entry.getValue();
                String srUniqueResourceKey = getUniqueResourceKey(tableInfoObj, catalog, schema, identity);
                log.info("ResourceKey srUniqueResourceKey " + srUniqueResourceKey);
                PepEvaluation evaluation = getAssetEvaluateResponseFromAPI(tableInfoObj, srUniqueResourceKey, identity);
                if (null != evaluation && TRANSFORM.equalsIgnoreCase(evaluation.getOutcome())) {
                    transformPolicyEvalResult(evaluation, tableInfoObj, policyFormatObj);
                    policyformatList.add(policyFormatObj);
                }
                else {
                    tableInfoObj.setColumnMetadataUnavailable(true);
                }
            }
        }
        return policyformatList;
    }

    private String getUniqueResourceKey(TableBasicInfo tableInfo, String catalog, String schema, Identity identity)
    {
        //String lhHostExtIpAddress = System.getenv("LH_HOST_IP_ADDRESS");
        //String lhHostPort = System.getenv("LH_HOST_PORT");

        String lhHostExtIpAddress = getPropertyValueFromWKCConfiguration(LH_HOST_IP_ADDRESS_PROPERTY_NAME);
        String lhHostPort = getPropertyValueFromWKCConfiguration(LH_HOST_PORT_PROPERTY_NAME);

        String strCatalog = StringHelperUtil.isNullString(tableInfo.getCatalogName()) ? catalog : tableInfo.getCatalogName();
        String strSchema = StringHelperUtil.isNullString(tableInfo.getSchemaName()) ? schema : tableInfo.getSchemaName();
        String strTable = tableInfo.getTableName();

        return lhHostExtIpAddress + PIPE_SYMBOL + lhHostPort + PIPE_SYMBOL + strCatalog + PIPE_SYMBOL + strSchema + PIPE_SYMBOL + strTable;
    }

    private void transformPolicyEvalResult(PepEvaluation evaluation, TableBasicInfo tableInfoObj, GovernPolicyFormat policyFormatObj)
    {
        log.info("======== transformPolicyEvalResult START ========");
        //String dpsDecisionOutputJson = "{\"outcome\": \"Transform\",\"transforms\": [{\"operation_type\":\"research/pseudo-pretty\",\"parameters\":[{\"maskingChar\":\"P\",\"maskingType\":\"Full\",\"name\":\"name\",\"preserveFormat\":\"false\",\"maskingLen\":\"10\"}],\"schemas\":[{\"name\":\"name\",\"type\":{\"length\":20.0,\"nullable\":true,\"type\":\"varchar\",\"scale\":0.0,\"signed\":false},\"order\":1}],\"profiled_dataclass\":\"65df5f9a-dafa-45e2-ac27-3accca24e595_dfd662b9-f6af-4af9-b8cc-014c3f59bbe1\"},{\"operation_type\":\"research/pseudo\",\"parameters\":[{\"salt\":\"034f0be01d6680b628a7fb5069e1b5f59b3c876ba3c2d65436db09b3e0e59b72\",\"dataclass\":\"EA\",\"maskingType\":\"Partial\",\"name\":\"email\",\"preserveFormat\":\"true\",\"maskingProcessor\":\"RepeatableFormatFabrication\",\"maskingOptions\":[{\"name\":\"User name\",\"value\":\"Generate user name\"},{\"name\":\"Domain name\",\"value\":\"Original\"}]}],\"schemas\":[{\"name\":\"email\",\"type\":{\"length\":50.0,\"nullable\":true,\"type\":\"varchar\",\"scale\":0.0,\"signed\":false},\"order\":3}],\"profiled_dataclass\":\"65df5f9a-dafa-45e2-ac27-3accca24e595_7d967c9c-fb56-4ac8-aa2b-637b848eac33\"},{\"operation_type\":\"description/transform_metadata\",\"parameters\":[{\"schema\":[{\"name\":\"stdid\",\"type\":{\"length\":10.0,\"nullable\":true,\"type\":\"integer\",\"scale\":0.0,\"signed\":true},\"order\":0},{\"name\":\"name\",\"type\":{\"length\":20.0,\"nullable\":true,\"type\":\"varchar\",\"scale\":0.0,\"signed\":false},\"order\":1},{\"name\":\"location\",\"type\":{\"length\":10.0,\"nullable\":true,\"type\":\"integer\",\"scale\":0.0,\"signed\":true},\"order\":2},{\"name\":\"email\",\"type\":{\"length\":50.0,\"nullable\":true,\"type\":\"varchar\",\"scale\":0.0,\"signed\":false},\"order\":3}],\"transformation_cost\":-1.0,\"transform_option\":\"advanced\",\"resource_key\":\"0000:0000:0000:0000:0000:FFFF:092E:50E3|8443|hive-hms:/testdemo2/student\",\"datasource_type\":\"11849f0a-54cc-448d-bb8c-d79206636e3d\",\"governance_convention\":\"AEAD\"}],\"schemas\":[]}]}";
        String dpsDecisionOutputJson = "{\"outcome\": \"Transform\",\"transforms\": " + evaluation.getTransformSpec() + "}";

        Map<String, String> maskParamMap = new HashMap<>();
        log.info("======== LtsConvertor START ========");
        LtsConvertor convertor = new LtsConvertor(true, LtsConvertor.OutputType.PARAM_CPP, dpsDecisionOutputJson);
        log.info("======== LtsConvertor END ========");
        Map<String, MaskSpecification> maskEntries = convertor.getSelectEntries();
        for (String columnName : maskEntries.keySet()) {
            log.info("WKC Debug MaskSpecification columnName" + columnName);
            MaskSpecification spec = maskEntries.get(columnName);
            //Workaround
            int columnLength = spec.getColumnLength();
            if (DATE_DATATYPE.equalsIgnoreCase(spec.getColumnDataType()) && columnLength == 0) {
                columnLength = 20;
            }
            String strAdditionalParam = COMMA_SYMBOL + spec.getMaskType() + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getMaskParams() + SINGLE_QUOTE_SYMBOL + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getFormat() + SINGLE_QUOTE_SYMBOL + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getSeed() + SINGLE_QUOTE_SYMBOL + COMMA_SYMBOL + columnLength + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getColumnDataType() + SINGLE_QUOTE_SYMBOL;
            log.info("WKC Debug strAdditionalParam" + strAdditionalParam);
            maskParamMap.put(columnName, strAdditionalParam);
        }

        enrichPolicyFormatObject(policyFormatObj, tableInfoObj, maskParamMap);

        //Adding Available Column List for Select * Scenario
        List<String> availableColumnList = getAvailableColumnList("{\"transforms\": " + evaluation.getTransformSpec() + "}");
        tableInfoObj.setAvailableColumnList(availableColumnList);
        log.info("======== transformPolicyEvalResult END ========");
    }

    /**
     * @param jsonString
     * @return
     */
    private List<String> getAvailableColumnList(String jsonString)
    {
        List<String> availableColumnList = new ArrayList<>();

        JsonReader reader = Json.createReader(new StringReader(jsonString));
        JsonObject json = reader.readObject();

        if (null != json && json.containsKey(TRANSFORMS)) {
            JsonArray transforms = json.getJsonArray(TRANSFORMS);

            if (null != transforms && !transforms.isEmpty()) {
                for (int i = 0; i < transforms.size(); i++) {
                    JsonObject transform = transforms.getJsonObject(i);
                    if (null != transform && null != transform.getString(OPERATION_TYPE)) {
                        String operationType = transform.getString(OPERATION_TYPE);
                        if (DESCRIPTION_TRANSFORM_METADATA.equals(operationType)) {
                            JsonArray parameters = transform.getJsonArray(PARAMETERS);
                            JsonObject transformMetadata = parameters.getJsonObject(0);
                            JsonArray schema = transformMetadata.getJsonArray(SCHEMA);
                            // Print all the "name" field values under the "schema" list
                            for (JsonValue schemaValue : schema) {
                                JsonObject schemaObj = (JsonObject) schemaValue;
                                String name = schemaObj.getString(NAME_VALUE);
                                availableColumnList.add(name);
                            }
                        }
                    }
                }
            }
        }
        return availableColumnList;
    }

    private void enrichPolicyFormatObject(GovernPolicyFormat governPolicyFormatObj, TableBasicInfo tableInfoObj, Map<String, String> maskParamMap)
    {
        governPolicyFormatObj.setTableName(tableInfoObj.getTableName());
        governPolicyFormatObj.setSchemaName(tableInfoObj.getSchemaName());
        governPolicyFormatObj.setCatalogName(tableInfoObj.getCatalogName());

        Map<String, UserDefinedFunctionInfo> maskInfoMap = new HashMap<>();

        //Need to recheck the Logic
        ArrayList<String> columnRuleNameList = new ArrayList<>();

        for (Map.Entry<String, String> entry : maskParamMap.entrySet()) {
            String strColumnName = entry.getKey();
            String strMaskParam = entry.getValue();
            UserDefinedFunctionInfo maskInfoObj = new UserDefinedFunctionInfo();
            maskInfoObj.setMethodName(MASK_METHOD_NAME);
            maskInfoObj.setAddOnParameters(strMaskParam);
            maskInfoMap.put(strColumnName, maskInfoObj);

            //Need to recheck the Logic
            columnRuleNameList.add(strColumnName);
        }
        governPolicyFormatObj.setColumns(columnRuleNameList);

        governPolicyFormatObj.setUserDefinedFunctionInfo(maskInfoMap);
    }

    private PepEvaluation getAssetEvaluateResponseFromAPI(TableBasicInfo tableInfoObj, String srUniqueResourceKey, Identity identity)
            throws QureyGovernanceException
    {
        log.info("======== getAssetEvaluateResponseFromAPI START ========");
        PepEvaluation evaluation = null;

        String bearerToken = getBearerTokenFromAPI();
        //System.out.println(bearerToken);

        //String dpsUri = "https://cpd-wkc.apps.testincjob.cp.fyre.ibm.com";
        String dpsUri = getPropertyValueFromWKCConfiguration(WKC_ENV_URL_PROPERTY_NAME);

        //String authorization = "Basic <service_token>";
        //String authorization = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRJT1RhSjdIYTBBVHM0WVRBN0pmQkE5Ulc3bmpqeGRYQWIzZ1Zxd2g3M3cifQ.eyJ1c2VybmFtZSI6ImFkbWluIiwicm9sZSI6IkFkbWluIiwicGVybWlzc2lvbnMiOlsibWFuYWdlX3ZhdWx0c19hbmRfc2VjcmV0cyIsInNoYXJlX3NlY3JldHMiLCJhZGRfdmF1bHRzIiwiYWRtaW5pc3RyYXRvciIsImNhbl9wcm92aXNpb24iLCJtb25pdG9yX3BsYXRmb3JtIiwiY29uZmlndXJlX3BsYXRmb3JtIiwidmlld19wbGF0Zm9ybV9oZWFsdGgiLCJjb25maWd1cmVfYXV0aCIsIm1hbmFnZV91c2VycyIsIm1hbmFnZV9ncm91cHMiLCJtYW5hZ2Vfc2VydmljZV9pbnN0YW5jZXMiLCJtYW5hZ2VfY2F0YWxvZyIsImNyZWF0ZV9wcm9qZWN0IiwiY3JlYXRlX3NwYWNlIiwiYXV0aG9yX2dvdmVybmFuY2VfYXJ0aWZhY3RzIiwibWFuYWdlX2dvdmVybmFuY2Vfd29ya2Zsb3ciLCJ2aWV3X2dvdmVybmFuY2VfYXJ0aWZhY3RzIiwibWFuYWdlX2NhdGVnb3JpZXMiLCJtYW5hZ2VfZ2xvc3NhcnkiLCJhY2Nlc3NfY2F0YWxvZyJdLCJncm91cHMiOlsxMDAwMF0sInN1YiI6ImFkbWluIiwiaXNzIjoiS05PWFNTTyIsImF1ZCI6IkRTWCIsInVpZCI6IjEwMDAzMzA5OTkiLCJhdXRoZW50aWNhdG9yIjoiZGVmYXVsdCIsImRpc3BsYXlfbmFtZSI6ImFkbWluIiwiaWF0IjoxNjc5NTAyOTQ3LCJleHAiOjE2Nzk1NDYxMTF9.oeInMzFmmL_pvXgVDCRQfYXt5dHbawlKWPu3ptL8erf_8dVRD8td9waS6P_myyH9xEGRfJmVucjca1TMREYMPqkYeX4zpq2XquIfEGeuBHjUJulls6n2WNkjKqpOFx0Fk6B8YQKscrr2nYl_TYQK7ekXxDqbycPgYhsX2JRgnZ2rGH1LiA62E0RaZzmiQr24irhn21TPclWBk6NRauWXjJcfl3hqkHH8zHL6bNMBTlPznF1tINWvBOnu9Xe4CHfed_sXOywfKUhwi4EyNZIZUXE84yJ0I5jBNJMvdalpfDPU9Je2UT-biOkCweDY9TwajPxwPrjxN1t1hHmxFxX_oQ";
        String authorization = "Bearer " + bearerToken;
        log.info("WKC Debug bearerToken" + bearerToken);
        //String catalogId = "13d23746-3852-4a75-8574-69ee8a65ed5d";
        //String assetId = "34f76936-1bc7-4bb9-9a8c-336a7a57d9a2";

        //String resourcePath = "8574-69ee8a65ed5d:/DB2INST1/EMPLOYEE";
        //String resourcePath = "0000:0000:0000:0000:0000:ffff:092E:50E3|8443|hive-hms|testdemo2|student";
        String resourcePath = srUniqueResourceKey;
        //String userId = "1000330999";
        String bssId = "999";

        PEP pep = null;
        try {
            // Setup PepConfig for Decision Cache
            PepConfiguration pepConfig = new PepConfiguration();
            pepConfig.setDpsUri(dpsUri);
            // This is only for Dev environment: (setRestSecurity.TRUST_ALL)
            pepConfig.setRestSecurity(com.ibm.wdp.policy.pep.PepConfiguration.RestSecurity.TRUST_ALL);
            pepConfig.setEnableCache(false); // default: true
            pepConfig.setCacheType(PepConfiguration.CacheType.DecisionCache); // default: CacheType.LRUPepCache
            pepConfig.setCacheSize(1000); // Default: 1000
            pepConfig.setCacheTTL(3600000); // default: 3600000; // 1 hour

            PepContext pepContext = new PepContext();
            //pepContext.setUser(userId);
            //pepContext.setCatalogId(catalogId);
            pepContext.setBssAccountId(bssId);

            // Set pep host type. Allowed values: [DV, DB2, CAMS, GUARDIUM, DATAFABRIC]
            pepContext.setPepHostType(PepContext.PEPHostType.GUARDIUM);

            if ("true".equalsIgnoreCase(getPropertyValueFromWKCConfiguration(WKC_QUERY_CONTEXT_CONFIG_USER_APPLICABLE))) {
                log.info("WKC Debug Config User Applicable TRUE");
                pepContext.setUser(getPropertyValueFromWKCConfiguration(WKC_QUERY_CONTEXT_CONFIG_USER_PROPERTY_NAME));
            }
            else {
                log.info("WKC Debug Config User Applicable FALSE");
                String userName = getUserNameFromIdentity(identity);
                pepContext.setUser(userName);
            }
            pepContext.setUserNameFlag(true);

            // Initialize PEP SDK for cached evaluation.
            pep = PEP.initialize(authorization, pepConfig);

            // Get PEP instance (singleton)
            pep = PEP.getInstance();

            // Register PEP instance - Not needed for now.
            // pep.registerPEP(PEPHostType.DV, "mjayapa-as-2.svl.ibm.com", "9.30.52.145");

            // Evaluate single item
            //PepEvaluation evaluation = pep.evaluateItem("Access", pepContext, assetId);
            //System.out.println("outcome:" + evaluation.getOutcome());

            // Evaluate single resource
            evaluation = pep.evaluateResource("Access", pepContext, resourcePath);
            //System.out.println("outcome:" + evaluation.getOutcome());
            //System.out.println(evaluation.getTransformSpec());
            log.info("======== getAssetEvaluateResponseFromAPI END ========");
        }
        catch (Exception e) {
            System.out.println("Exception inside getAssetEvaluateResponseFromAPI");
            log.info("======== Exception inside getAssetEvaluateResponseFromAPI ========" + e);
        }
        finally {
            // Terminate PEP SDK
            try {
                pep.terminate();
            }
            catch (Exception ex) {
                System.out.println("123");
                log.info("======== Exception inside getAssetEvaluateResponseFromAPI Finally========" + ex);
            }
        }
        return evaluation;
    }

    /**
     * This method will extract User Details From Identity Object
     *
     * @param identity
     * @return
     */
    private String getUserNameFromIdentity(Identity identity)
    {
        String userName = identity.getUser();
        if (identity.getPrincipal().isPresent()) {
            Principal principalObj = identity.getPrincipal().get();
            userName = principalObj.getName();
        }
        return userName;
    }

    private static String getBearerTokenFromAPI()
    {
        try {
            //URL url = new URL("https://cpd-wkc.apps.testincjob.cp.fyre.ibm.com/icp4d-api/v1/authorize");

            URL url = new URL(getPropertyValueFromWKCConfiguration(WKC_ENV_URL_PROPERTY_NAME) + "/icp4d-api/v1/authorize");

            //disableSSLVerification();

            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
            httpConn.setRequestMethod("POST");

            httpConn.setRequestProperty("Content-Type", "application/json");
            //httpConn.setRequestProperty("Cookie", "1e7fba00fc13fda922f1da74a484e589=7d22607a044d942f639020c5e59f3206");

            httpConn.setDoOutput(true);
            OutputStreamWriter writer = new OutputStreamWriter(httpConn.getOutputStream());
            //writer.write("{\"username\":\"admin\",\"password\":\"kE1OMptxx5Nu\"}");
            writer.write("{\"username\":\"" + getPropertyValueFromWKCConfiguration(WKC_ENV_USR_NAME_PROPERTY_NAME) + "\",\"password\":\"" + getPropertyValueFromWKCConfiguration(WKC_ENV_USR_PASSWORD_PROPERTY_NAME) + "\"}");
            writer.flush();
            writer.close();
            httpConn.getOutputStream().close();

            InputStream responseStream = httpConn.getResponseCode() / 100 == 2 ? httpConn.getInputStream()
                    : httpConn.getErrorStream();
            Scanner s = new Scanner(responseStream).useDelimiter("\\A");
            String response = s.hasNext() ? s.next() : "";

            if (null != response && !"".equalsIgnoreCase(response)) {
                JsonObject jobj = convertToJsonObject(response);
                //JsonObject jobj = new Gson().fromJson(response, JsonObject.class);
                if (null != jobj && null != jobj.get(TOKEN)) {
                    String bearerToken = jobj.get(TOKEN).toString();
                    bearerToken = bearerToken.replaceAll(ESCAPE_DOUBLE_QUOTES, EMPTY_STRING);
                    return bearerToken;
                }
            }

            // System.out.println(response);
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

    public static void disableSSLVerification()
            throws Exception
    {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager()
        {
            public void checkClientTrusted(X509Certificate[] chain, String authType)
            {
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType)
            {
            }

            public X509Certificate[] getAcceptedIssuers()
            {
                return null;
            }
        }};

        // Install the trust manager
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());

        // Disable hostname verification
        HttpsURLConnection.setDefaultHostnameVerifier((hostname, sslSession) -> true);
    }

    public static JsonObject convertToJsonObject(String jsonString)
    {
        try {
            // Create a JsonReaderFactory instance
            JsonReaderFactory factory = Json.createReaderFactory(null);
            // Create a StringReader from the input JSON string
            StringReader stringReader = new StringReader(jsonString);
            // Create a JsonReader from the JsonReaderFactory
            JsonReader jsonReader = factory.createReader(stringReader);
            // Use the JsonReader to parse the JSON string and return a JsonObject
            return jsonReader.readObject();
        }
        catch (JsonParsingException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * This method will load WKC Configuration from the configuration file
     *
     * @param propertyName : Property Name
     * @return : String
     */
    public static String getPropertyValueFromWKCConfiguration(String propertyName)
    {
        String propertyValue = null;
        try {
            Map<String, String> properties = loadProperties(WKC_CONFIG_PROPERTIES);
            properties = new HashMap<>(properties);
            checkArgument(!isNullOrEmpty(properties.get(propertyName)),
                    "Bearer Token configuration %s does not contain %s",
                    WKC_CONFIG_PROPERTIES.getAbsoluteFile(),
                    propertyName);

            propertyValue = properties.remove(propertyName);
            checkArgument(!isNullOrEmpty(propertyValue), "%s property must be present", propertyName);
        }
        catch (IOException e) {
            throw new QureyGovernanceException(e.getMessage());
        }
        return propertyValue;
    }
}
