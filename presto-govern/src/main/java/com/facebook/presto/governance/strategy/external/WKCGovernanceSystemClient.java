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
    private static final String WKC_ENV_FYRE = "wkc-env-fyre";
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
        String lhHostExtIpAddress = getPropertyValueFromWKCConfiguration(LH_HOST_IP_ADDRESS_PROPERTY_NAME);
        String lhHostPort = getPropertyValueFromWKCConfiguration(LH_HOST_PORT_PROPERTY_NAME);

        String strCatalog = StringHelperUtil.isNullString(tableInfo.getCatalogName()) ? catalog : tableInfo.getCatalogName();
        String strSchema = StringHelperUtil.isNullString(tableInfo.getSchemaName()) ? schema : tableInfo.getSchemaName();
        String strTable = tableInfo.getTableName();

        return lhHostExtIpAddress + PIPE_SYMBOL + lhHostPort + PIPE_SYMBOL + strCatalog + PIPE_SYMBOL + strSchema + PIPE_SYMBOL + strTable;
    }

    private void transformPolicyEvalResult(PepEvaluation evaluation, TableBasicInfo tableInfoObj, GovernPolicyFormat policyFormatObj)
    {
        log.debug("======== transformPolicyEvalResult START ========");
        String dpsDecisionOutputJson = "{\"outcome\": \"Transform\",\"transforms\": " + evaluation.getTransformSpec() + "}";

        Map<String, String> maskParamMap = new HashMap<>();
        log.debug("======== LtsConvertor START ========");
        LtsConvertor convertor = new LtsConvertor(true, LtsConvertor.OutputType.PARAM_CPP, dpsDecisionOutputJson);
        log.debug("======== LtsConvertor END ========");
        Map<String, MaskSpecification> maskEntries = convertor.getSelectEntries();
        for (String columnName : maskEntries.keySet()) {
            log.debug("WKC Debug MaskSpecification columnName" + columnName);
            MaskSpecification spec = maskEntries.get(columnName);
            //Workaround
            int columnLength = spec.getColumnLength();
            if (DATE_DATATYPE.equalsIgnoreCase(spec.getColumnDataType()) && columnLength == 0) {
                columnLength = 20;
            }
            String strAdditionalParam = COMMA_SYMBOL + spec.getMaskType() + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getMaskParams() + SINGLE_QUOTE_SYMBOL + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getFormat() + SINGLE_QUOTE_SYMBOL + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getSeed() + SINGLE_QUOTE_SYMBOL + COMMA_SYMBOL + columnLength + COMMA_SYMBOL + SINGLE_QUOTE_SYMBOL + spec.getColumnDataType() + SINGLE_QUOTE_SYMBOL;
            log.debug("WKC Debug strAdditionalParam" + strAdditionalParam);
            maskParamMap.put(columnName, strAdditionalParam);
        }

        enrichPolicyFormatObject(policyFormatObj, tableInfoObj, maskParamMap);

        //Adding Available Column List for Select * Scenario
        List<String> availableColumnList = getAvailableColumnList("{\"transforms\": " + evaluation.getTransformSpec() + "}");
        tableInfoObj.setAvailableColumnList(availableColumnList);
        log.debug("======== transformPolicyEvalResult END ========");
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
        log.debug("======== getAssetEvaluateResponseFromAPI START ========");
        PepEvaluation evaluation = null;

        String bearerToken = getBearerTokenFromAPI();

        String dpsUri = getPropertyValueFromWKCConfiguration(WKC_ENV_URL_PROPERTY_NAME);

        String authorization = "Bearer " + bearerToken;
        log.info("WKC Debug bearerToken" + bearerToken);
        String resourcePath = srUniqueResourceKey;
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
            pepContext.setBssAccountId(bssId);

            // Set pep host type. Allowed values: [DV, DB2, CAMS, GUARDIUM, DATAFABRIC]
            pepContext.setPepHostType(PepContext.PEPHostType.GUARDIUM);

            /*if ("true".equalsIgnoreCase(getPropertyValueFromWKCConfiguration(WKC_QUERY_CONTEXT_CONFIG_USER_APPLICABLE))) {
                log.info("WKC Debug Config User Applicable TRUE");
                pepContext.setUser(getPropertyValueFromWKCConfiguration(WKC_QUERY_CONTEXT_CONFIG_USER_PROPERTY_NAME));
            }
            else {
                log.info("WKC Debug Config User Applicable FALSE");
                String userName = getUserNameFromIdentity(identity);
                pepContext.setUser(userName);
            }*/
            String userName = getUserNameFromIdentity(identity);
            pepContext.setUser(userName);
            pepContext.setUserNameFlag(true);

            // Initialize PEP SDK for cached evaluation.
            pep = PEP.initialize(authorization, pepConfig);

            // Get PEP instance (singleton)
            pep = PEP.getInstance();

            // Evaluate single resource
            evaluation = pep.evaluateResource("Access", pepContext, resourcePath);
            log.debug("======== getAssetEvaluateResponseFromAPI END ========");
        }
        catch (Exception e) {
            log.error("======== Exception inside getAssetEvaluateResponseFromAPI ========" + e);
            throw new QureyGovernanceException("Exception in data protection policy management check " + e.getMessage());
        }
        finally {
            try {
                pep.terminate();
            }
            catch (Exception ex) {
                log.error("======== Exception inside getAssetEvaluateResponseFromAPI Finally========" + ex);
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
            URL url = new URL(getPropertyValueFromWKCConfiguration(WKC_ENV_URL_PROPERTY_NAME) + "/icp4d-api/v1/authorize");

            if ("true".equalsIgnoreCase(getPropertyValueFromWKCConfiguration(WKC_ENV_FYRE))) {
                disableSSLVerification();
            }

            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
            httpConn.setRequestMethod("POST");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setDoOutput(true);
            OutputStreamWriter writer = new OutputStreamWriter(httpConn.getOutputStream());
            String wkcAdminUserName = System.getenv("WKC_ADMIN_USER");
            String wkcAdminPassword = System.getenv("WKC_ADMIN_PASS");
            if (StringHelperUtil.isNullString(wkcAdminUserName) || StringHelperUtil.isNullString(wkcAdminPassword)) {
                throw new QureyGovernanceException("WKC Credentials are not available and is mandatory as WKC Policy Enforcement is enabled");
            }
            //String wkcAdminUserName = getPropertyValueFromWKCConfiguration(WKC_ENV_USR_NAME_PROPERTY_NAME);
            //String wkcAdminPassword = getPropertyValueFromWKCConfiguration(WKC_ENV_USR_PASSWORD_PROPERTY_NAME);
            writer.write("{\"username\":\"" + wkcAdminUserName + "\",\"password\":\"" + wkcAdminPassword + "\"}");
            writer.flush();
            writer.close();
            httpConn.getOutputStream().close();

            InputStream responseStream = httpConn.getResponseCode() / 100 == 2 ? httpConn.getInputStream()
                    : httpConn.getErrorStream();
            Scanner s = new Scanner(responseStream).useDelimiter("\\A");
            String response = s.hasNext() ? s.next() : "";

            if (null != response && !"".equalsIgnoreCase(response)) {
                JsonObject jobj = convertToJsonObject(response);
                if (null != jobj && null != jobj.get(TOKEN)) {
                    String bearerToken = jobj.get(TOKEN).toString();
                    bearerToken = bearerToken.replaceAll(ESCAPE_DOUBLE_QUOTES, EMPTY_STRING);
                    return bearerToken;
                }
            }
        }
        catch (QureyGovernanceException ex) {
            throw ex;
        }
        catch (IllegalArgumentException iex) {
            throw new QureyGovernanceException(iex.getMessage());
        }
        catch (Exception e) {
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
                    "WKC configuration %s does not contain mandatory configuration %s , This configuration is mandatory as WKC Policy Governance is enabled",
                    WKC_CONFIG_PROPERTIES.getPath(),
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
