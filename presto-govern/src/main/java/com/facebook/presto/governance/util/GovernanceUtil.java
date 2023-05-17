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
package com.facebook.presto.governance.util;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.governance.entity.ColumnDetails;
import com.facebook.presto.governance.entity.GovernPolicyFormat;
import com.facebook.presto.governance.entity.QueryBasicInfo;
import com.facebook.presto.governance.entity.QueryDetails;
import com.facebook.presto.governance.entity.QueryRewriteApplicableResponse;
import com.facebook.presto.governance.entity.TableBasicInfo;
import com.facebook.presto.governance.entity.TableDetails;
import com.facebook.presto.governance.entity.UserDefinedFunctionInfo;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class GovernanceUtil
{
    public static final String IMPLICIT_JOIN = "IMPLICIT_JOIN";
    public static final String INNER_JOIN = "INNER_JOIN";
    public static final String LEFT_JOIN = "LEFT_JOIN";
    public static final String RIGHT_JOIN = "RIGHT_JOIN";
    public static final String FULL_OUTER_JOIN = "FULL_OUTER_JOIN";
    public static final String SIMPLE = "SIMPLE";
    public static final String IMPLICIT = "IMPLICIT";
    public static final String INNER = "INNER";
    public static final String LEFT = "LEFT";
    public static final String RIGHT = "RIGHT";
    public static final String FULL = "FULL";
    public static final String ID_TYPE = "ID_TYPE";
    public static final String DEREF_TYPE = "DEREF_TYPE";
    public static final String TABLE_NAME = "TABLE_NAME";
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String FROM_CLAUSE = "from";
    public static final String EMPTY_STRING = "";
    public static final String SCHEMA_NAME = "SCHEMA_NAME";
    public static final String CATALOG_NAME = "CATALOG_NAME";
    public static final String DOT_SYMBOL = ".";

    public static final String DEFAULT = "system.default";
    public static final String DOUBLE_QUOTES_SYMBOL = "\"";

    public static final String COMMA_SYMBOL = ",";
    public static final String STAR_SYMBOL = "*";

    Logger log = Logger.get(GovernanceUtil.class);

    /**
     * This method will analyze and decides whether query rewrite is required for the given query as per the policies
     *
     * @param wrappedStatement
     * @param policyformatList
     * @param queryBasicInfo
     * @return QueryRewriteApplicableResponse
     */
    public QueryRewriteApplicableResponse isQueryReWriteRequired(Statement wrappedStatement, List<GovernPolicyFormat> policyformatList, QueryBasicInfo queryBasicInfo)
    {
        QueryRewriteApplicableResponse queryRewriteApplicableResponse = new QueryRewriteApplicableResponse();
        if (wrappedStatement instanceof Query) {
            List<GovernPolicyFormat> applicablePolicies = new ArrayList<>();
            QueryDetails queryDetails = extractTableAndColumnDetails(wrappedStatement, queryBasicInfo);

            if (null != policyformatList && !policyformatList.isEmpty()) {
                policyformatList.stream().forEach(governPolicyFormatObj -> {
                    if (null != governPolicyFormatObj.getTableName() && queryDetails.getQueryTableDetails().containsKey(governPolicyFormatObj.getTableName())) {
                        TableDetails currTableDetails = queryDetails.getQueryTableDetails().get(governPolicyFormatObj.getTableName());
                        List<String> applicableColumnList = new ArrayList<>();
                        if (null != currTableDetails.getColumnDetList()) {
                            applicableColumnList.addAll(currTableDetails.getColumnDetList().stream().map(colObj -> colObj.getColumnName()).collect(Collectors.toList()));
                        }
                        if (null != currTableDetails.getCommonColumnDetList()) {
                            applicableColumnList.addAll(currTableDetails.getCommonColumnDetList().stream().map(colObj -> colObj.getColumnName()).collect(Collectors.toList()));
                        }
                        //Need to check and refine Logic
                        governPolicyFormatObj.getColumns().stream().forEach(strColumnName -> {
                            if (applicableColumnList.contains(strColumnName) && !applicablePolicies.contains(governPolicyFormatObj)) {
                                applicablePolicies.add(governPolicyFormatObj);
                            }
                        });
                    }
                });
            }

            if (null != applicablePolicies && !applicablePolicies.isEmpty()) {
                queryRewriteApplicableResponse.setRewriteApplicable(true);
                queryRewriteApplicableResponse.setApplicablePolicies(applicablePolicies);
                queryRewriteApplicableResponse.setQueryDetails(queryDetails);
            }
        }
        return queryRewriteApplicableResponse;
    }

    /**
     * This method will enrich QueryRewriteApplicableResponse with Column Masking information
     *
     * @param queryRewriteApplicableResponse
     */
    public void enrichColumnMaskInformation(QueryRewriteApplicableResponse queryRewriteApplicableResponse)
    {
        QueryDetails queryDetails = queryRewriteApplicableResponse.getQueryDetails();
        Map<String, TableDetails> queryTableDetailsMap = queryDetails.getQueryTableDetails();

        List<GovernPolicyFormat> applicablePolicies = queryRewriteApplicableResponse.getApplicablePolicies();
        applicablePolicies.stream().forEach(governPolicyFormatObj -> {
            TableDetails tableDetailsObj = queryTableDetailsMap.get(governPolicyFormatObj.getTableName());
            List<ColumnDetails> columnDetList = tableDetailsObj.getColumnDetList();
            List<ColumnDetails> commonColumnDetList = tableDetailsObj.getCommonColumnDetList();
            if (tableDetailsObj.getTableName().equalsIgnoreCase(governPolicyFormatObj.getTableName())) {
                Map<String, UserDefinedFunctionInfo> maskInfo = governPolicyFormatObj.getUserDefinedFunctionInfo();
                maskInfo.entrySet().stream().forEach(entry -> {
                    String columnName = entry.getKey();
                    UserDefinedFunctionInfo currMaskObj = entry.getValue();
                    if (null != columnDetList && !columnDetList.isEmpty()) {
                        List<ColumnDetails> columnDetailsFilterList = columnDetList.stream().filter(columnObj -> columnName.equalsIgnoreCase(columnObj.getColumnName())).collect(Collectors.toList());
                        if (null != columnDetailsFilterList && !columnDetailsFilterList.isEmpty()) {
                            columnDetailsFilterList.stream().forEach(columnObj -> {
                                currMaskObj.setReferenceReplaceName(columnObj.getReferredName());
                                currMaskObj.setAliasName(columnObj.getAliasName());
                                currMaskObj.setColumnName(columnObj.getColumnName());
                            });
                        }
                    }

                    if (null != commonColumnDetList && !commonColumnDetList.isEmpty()) {
                        List<ColumnDetails> columnCommonDetailsFilterList = commonColumnDetList.stream().filter(columnObj -> columnName.equalsIgnoreCase(columnObj.getColumnName())).collect(Collectors.toList());
                        if (null != columnCommonDetailsFilterList && !columnCommonDetailsFilterList.isEmpty()) {
                            columnCommonDetailsFilterList.stream().forEach(columnObj -> {
                                currMaskObj.setReferenceReplaceName(columnObj.getReferredName());
                                currMaskObj.setColumnName(columnObj.getColumnName());
                            });
                        }
                    }
                });
            }
        });
    }

    /**
     * This method will modfiy the input query as a new rewritten query as per the policy
     *
     * @param query input query
     * @param applicablePolicies applicable policies to the query
     * @param queryBasicInfo
     * @return modified rewritten query
     */
    public String getModifiedRewrittenQuery(String query, List<GovernPolicyFormat> applicablePolicies, QueryBasicInfo queryBasicInfo)
    {
        log.info("======== getModifiedRewrittenQuery ========");
        Set<String> replacedValueSet = new HashSet<>();
        query = query.toLowerCase(Locale.ENGLISH);
        String selectQuery = query;
        String predicate = EMPTY_STRING;

        if (query.contains(FROM_CLAUSE)) {
            selectQuery = query.substring(0, query.indexOf(FROM_CLAUSE));
            predicate = query.substring(query.indexOf(FROM_CLAUSE));
        }

        for (GovernPolicyFormat governPolicyFormatObj : applicablePolicies) {
            Map<String, UserDefinedFunctionInfo> maskInfo = governPolicyFormatObj.getUserDefinedFunctionInfo();
            for (Map.Entry<String, UserDefinedFunctionInfo> entry : maskInfo.entrySet()) {
                String columnName = entry.getKey();
                UserDefinedFunctionInfo currObj = entry.getValue();
                if (!StringHelperUtil.isNullString(currObj.getReferenceReplaceName()) && !replacedValueSet.contains(currObj.getReferenceReplaceName())) {
                    String replaceValue = currObj.getMethodName() + "(" + currObj.getReferenceReplaceName() + currObj.getAddOnParameters() + ") " + ((!StringHelperUtil.isNullString(currObj.getAliasName())) ? "" : columnName);
                    //selectQuery = selectQuery.replaceAll(currObj.getReferenceReplaceName(), replaceValue);
                    selectQuery = selectQuery.replaceAll(currObj.getReferenceReplaceName(), Matcher.quoteReplacement(replaceValue));
                    replacedValueSet.add(currObj.getReferenceReplaceName());
                }
            }
        }
        String newQuery = selectQuery + predicate;
        log.info("======== COMPLETED getModifiedRewrittenQuery ========");
        log.info("newQuery" + newQuery);
        return newQuery;
    }

    /**
     * This method will extract Table and Column Details
     *
     * @param wrappedStatement
     * @param queryBasicInfo
     * @return
     */
    private QueryDetails extractTableAndColumnDetails(Statement wrappedStatement, QueryBasicInfo queryBasicInfo)
    {
        QueryDetails queryDetails = EnrichQueryTableDetailsFromWrappedStatement(wrappedStatement, queryBasicInfo);
        enrichQueryObjectWithColumnDetails(wrappedStatement, queryDetails);
        return queryDetails;
    }

    /**
     * This method will enrich Query Object with Column Details
     *
     * @param wrappedStatement
     * @param queryDetails
     * @return queryDetails
     */
    private QueryDetails enrichQueryObjectWithColumnDetails(Statement wrappedStatement, QueryDetails queryDetails)
    {
        QuerySpecification querySpecification = (QuerySpecification) ((Query) wrappedStatement).getQueryBody();
        List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();
        // SELECT * Condition
        if (selectItems.stream().anyMatch(AllColumns.class::isInstance)) {
            if (SIMPLE.equalsIgnoreCase(queryDetails.getQueryType()) || IMPLICIT_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || INNER_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || LEFT_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || RIGHT_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || FULL_OUTER_JOIN.equalsIgnoreCase(queryDetails.getQueryType())) {
                List<ColumnDetails> columnDetailsList = new ArrayList<>();
                queryDetails.getQueryTableDetails().entrySet().stream().forEach(currTableObj -> {
                    TableDetails tableObject = currTableObj.getValue();
                    String strCatalogName = tableObject.getCatalogName();
                    String strSchemaName = tableObject.getSchemaName();
                    String strTableName = tableObject.getTableName();
                    List<String> availableColumnList = tableObject.getAvailableColumnList();
                    //String referredTableName = DOUBLE_QUOTES_SYMBOL + strCatalogName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + strSchemaName + DOT_SYMBOL + strTableName;
                    String referredTableName = DOUBLE_QUOTES_SYMBOL + strCatalogName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strSchemaName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strTableName + DOUBLE_QUOTES_SYMBOL;
                    if (!tableObject.isColumnMetadataUnavailable()) {
                        for (String strColumnName : availableColumnList) {
                            ColumnDetails columnDetails = new ColumnDetails();
                            columnDetails.setColumnName(strColumnName);
                            columnDetails.setReferenceType(ID_TYPE);
                            //columnDetails.setAliasName(strColumnName);
                            columnDetails.setReferredName(referredTableName + DOT_SYMBOL + strColumnName);
                            columnDetailsList.add(columnDetails);
                        }
                        tableObject.setColumnDetList(columnDetailsList);
                    }
                });
            }

            return queryDetails;
        }
        else {
            if (SIMPLE.equalsIgnoreCase(queryDetails.getQueryType())) {
                List<ColumnDetails> columnDetailsList = new ArrayList<>();
                for (int i = 0; i < selectItems.size(); i++) {
                    ColumnDetails columnDetails = new ColumnDetails();
                    SingleColumn singleColumn = (SingleColumn) selectItems.get(i);
                    String strColumnName = getTableOrColumnNameFromSingleColumnObject(singleColumn, COLUMN_NAME);
                    String strTableName = getTableOrColumnNameFromSingleColumnObject(singleColumn, TABLE_NAME);
                    String strSchemaName = getTableOrColumnNameFromSingleColumnObject(singleColumn, SCHEMA_NAME);
                    String strCatalogName = getTableOrColumnNameFromSingleColumnObject(singleColumn, CATALOG_NAME);
                    String strAliasName = getAliasNameFromSingleColumnObject(singleColumn);
                    String referredColumnName = null;
                    if (!StringHelperUtil.isNullString(strCatalogName) && !StringHelperUtil.isNullString(strSchemaName) && !StringHelperUtil.isNullString(strTableName)) {
                        referredColumnName = strCatalogName + DOT_SYMBOL + strSchemaName + DOT_SYMBOL + strTableName + DOT_SYMBOL + strColumnName;
                    }
                    else if (!StringHelperUtil.isNullString(strSchemaName) && !StringHelperUtil.isNullString(strTableName)) {
                        referredColumnName = strSchemaName + DOT_SYMBOL + strTableName + DOT_SYMBOL + strColumnName;
                    }
                    else if (!StringHelperUtil.isNullString(strTableName)) {
                        referredColumnName = strTableName + DOT_SYMBOL + strColumnName;
                    }
                    else {
                        referredColumnName = strColumnName;
                    }
                    columnDetails.setColumnName(strColumnName);
                    columnDetails.setReferenceType(ID_TYPE);
                    columnDetails.setAliasName(strAliasName);
                    columnDetails.setReferredName(referredColumnName);
                    columnDetailsList.add(columnDetails);
                }
                queryDetails.getQueryTableDetails().entrySet().stream().forEach(currTableObj -> {
                    TableDetails tableObject = currTableObj.getValue();
                    tableObject.setColumnDetList(columnDetailsList);
                });
            }
            else if (IMPLICIT_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || INNER_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || LEFT_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || RIGHT_JOIN.equalsIgnoreCase(queryDetails.getQueryType()) || FULL_OUTER_JOIN.equalsIgnoreCase(queryDetails.getQueryType())) {
                Map<String, List<ColumnDetails>> fullColumnDetailsMap = new HashMap<>();
                List<ColumnDetails> commonColumnList = new ArrayList<>();
                for (int i = 0; i < selectItems.size(); i++) {
                    ColumnDetails columnDetails = new ColumnDetails();
                    SingleColumn singleColumn = (SingleColumn) selectItems.get(i);
                    String columnRefType = getReferenceTypeFromSingleColumnObject(singleColumn);
                    if (ID_TYPE.equalsIgnoreCase(columnRefType)) {
                        String strColumnName = getTableOrColumnNameFromSingleColumnObject(singleColumn, COLUMN_NAME);
                        String strTableName = getTableOrColumnNameFromSingleColumnObject(singleColumn, TABLE_NAME);
                        String strSchemaName = getTableOrColumnNameFromSingleColumnObject(singleColumn, SCHEMA_NAME);
                        String strCatalogName = getTableOrColumnNameFromSingleColumnObject(singleColumn, CATALOG_NAME);
                        String strAliasName = getAliasNameFromSingleColumnObject(singleColumn);
                        String referredColumnName = null;
                        if (!StringHelperUtil.isNullString(strCatalogName) && !StringHelperUtil.isNullString(strSchemaName) && !StringHelperUtil.isNullString(strTableName)) {
                            referredColumnName = strCatalogName + DOT_SYMBOL + strSchemaName + DOT_SYMBOL + strTableName + DOT_SYMBOL + strColumnName;
                        }
                        else if (!StringHelperUtil.isNullString(strSchemaName) && !StringHelperUtil.isNullString(strTableName)) {
                            referredColumnName = strSchemaName + DOT_SYMBOL + strTableName + DOT_SYMBOL + strColumnName;
                        }
                        else if (!StringHelperUtil.isNullString(strTableName)) {
                            referredColumnName = strTableName + DOT_SYMBOL + strColumnName;
                        }
                        else {
                            referredColumnName = strColumnName;
                        }
                        columnDetails.setColumnName(strColumnName);
                        columnDetails.setReferenceType(ID_TYPE);
                        columnDetails.setAliasName(strAliasName);
                        columnDetails.setReferredName(referredColumnName);
                        commonColumnList.add(columnDetails);
                    }
                    else if (DEREF_TYPE.equalsIgnoreCase(columnRefType)) {
                        String strColumnName = getTableOrColumnNameFromSingleColumnObject(singleColumn, COLUMN_NAME);
                        String strTableName = getTableOrColumnNameFromSingleColumnObject(singleColumn, TABLE_NAME);
                        String strSchemaName = getTableOrColumnNameFromSingleColumnObject(singleColumn, SCHEMA_NAME);
                        String strCatalogName = getTableOrColumnNameFromSingleColumnObject(singleColumn, CATALOG_NAME);
                        String strAliasName = getAliasNameFromSingleColumnObject(singleColumn);
                        String referredTableName = null;
                        if (!StringHelperUtil.isNullString(strCatalogName) && !StringHelperUtil.isNullString(strSchemaName) && !StringHelperUtil.isNullString(strTableName)) {
                            referredTableName = strCatalogName + DOT_SYMBOL + strSchemaName + DOT_SYMBOL + strTableName;
                        }
                        else if (!StringHelperUtil.isNullString(strSchemaName) && !StringHelperUtil.isNullString(strTableName)) {
                            referredTableName = strSchemaName + DOT_SYMBOL + strTableName;
                        }
                        else if (!StringHelperUtil.isNullString(strTableName)) {
                            referredTableName = strTableName;
                        }
                        columnDetails.setColumnName(strColumnName);
                        columnDetails.setAliasName(strAliasName);
                        columnDetails.setReferredName(referredTableName + DOT_SYMBOL + strColumnName);
                        columnDetails.setReferenceType(DEREF_TYPE);
                        if (fullColumnDetailsMap.containsKey(strTableName)) {
                            fullColumnDetailsMap.get(strTableName).add(columnDetails);
                        }
                        else {
                            List<ColumnDetails> columnDetailsList = new ArrayList<>();
                            columnDetailsList.add(columnDetails);
                            fullColumnDetailsMap.put(strTableName, columnDetailsList);
                        }
                    }
                }
                queryDetails.getQueryTableDetails().entrySet().stream().forEach(currTableObj -> {
                    TableDetails tableObject = currTableObj.getValue();
                    tableObject.setColumnDetList(fullColumnDetailsMap.get(tableObject.getTableName()));
                    tableObject.setCommonColumnDetList(commonColumnList);
                });
            }

            return queryDetails;
        }
    }

    /**
     * This method will extract Table or Column name from the SingleColumn Object as per input entityType
     *
     * @param singleColumn : Single Column Object
     * @param entityType : TABLE/COLUMN
     * @return : Table Name / Column Name
     */
    private String getTableOrColumnNameFromSingleColumnObject(SingleColumn singleColumn, String entityType)
    {
        Expression expression = singleColumn.getExpression();
        if (entityType.equalsIgnoreCase(COLUMN_NAME) && expression instanceof Identifier) {
            Identifier id = (Identifier) expression;
            return getExactQueryReferredValue(id);
        }
        else if (expression instanceof DereferenceExpression) {
            DereferenceExpression defExpression = (DereferenceExpression) expression;
            if (COLUMN_NAME.equalsIgnoreCase(entityType)) {
                if (null != defExpression.getField() && defExpression.getField() instanceof Identifier) {
                    Identifier id = defExpression.getField();
                    return getExactQueryReferredValue(id);
                }
            }
            else if (TABLE_NAME.equalsIgnoreCase(entityType)) {
                if (null != defExpression.getBase() && defExpression.getBase() instanceof Identifier) {
                    Identifier id = (Identifier) defExpression.getBase();
                    return id.getValue();
                }
                else if (null != defExpression.getBase() && defExpression.getBase() instanceof DereferenceExpression) {
                    DereferenceExpression defExpressionlvl2 = (DereferenceExpression) defExpression.getBase();
                    if (defExpressionlvl2.getField() instanceof Identifier) {
                        Identifier id = defExpressionlvl2.getField();
                        return getExactQueryReferredValue(id);
                    }
                }
            }
            else if (SCHEMA_NAME.equalsIgnoreCase(entityType)) {
                if (null != defExpression.getBase() && defExpression.getBase() instanceof DereferenceExpression) {
                    DereferenceExpression defExpressionlvl2 = (DereferenceExpression) defExpression.getBase();
                    if (defExpressionlvl2.getBase() instanceof Identifier) {
                        if (null != defExpressionlvl2.getBase() && defExpressionlvl2.getBase() instanceof Identifier) {
                            Identifier id = (Identifier) defExpressionlvl2.getBase();
                            return getExactQueryReferredValue(id);
                        }
                    }
                    else if (defExpressionlvl2.getBase() instanceof DereferenceExpression) {
                        DereferenceExpression defExpressionlvl3 = (DereferenceExpression) defExpressionlvl2.getBase();
                        if (null != defExpressionlvl3.getField() && defExpressionlvl2.getField() instanceof Identifier) {
                            Identifier id = defExpressionlvl3.getField();
                            return getExactQueryReferredValue(id);
                        }
                    }
                }
            }
            else if (CATALOG_NAME.equalsIgnoreCase(entityType)) {
                if (null != defExpression.getBase() && defExpression.getBase() instanceof DereferenceExpression) {
                    DereferenceExpression defExpressionlvl2 = (DereferenceExpression) defExpression.getBase();
                    if (defExpressionlvl2.getBase() instanceof DereferenceExpression) {
                        DereferenceExpression defExpressionlvl3 = (DereferenceExpression) defExpressionlvl2.getBase();
                        if (null != defExpressionlvl3.getBase() && defExpressionlvl3.getBase() instanceof Identifier) {
                            Identifier id = (Identifier) defExpressionlvl3.getBase();
                            return getExactQueryReferredValue(id);
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * This method will return the exact referred value of an identifier
     *
     * @param id : Identifier
     * @return : String - Referred Value
     */
    private String getExactQueryReferredValue(Identifier id)
    {
        return (id.isDelimited()) ? DOUBLE_QUOTES_SYMBOL + id.getValue() + DOUBLE_QUOTES_SYMBOL : id.getValue();
    }

    /**
     * This method will return Column Alias Name from Single Column Object
     *
     * @param singleColumn
     * @return Alias Name
     */
    private String getAliasNameFromSingleColumnObject(SingleColumn singleColumn)
    {
        Optional<Identifier> alias = singleColumn.getAlias();
        if (alias.isPresent()) {
            return String.valueOf(alias.get());
        }
        return null;
    }

    /**
     * This method will return Column Reference Type from the SingleColumn Object
     *
     * @param singleColumn
     * @return
     */
    private String getReferenceTypeFromSingleColumnObject(SingleColumn singleColumn)
    {
        Expression expression = singleColumn.getExpression();
        if (expression instanceof Identifier) {
            Identifier id = (Identifier) expression;
            return ID_TYPE;
        }
        else if (expression instanceof DereferenceExpression) {
            DereferenceExpression defExpression = (DereferenceExpression) expression;
            return DEREF_TYPE;
        }
        return null;
    }

    /**
     * This method is used to extract input query information from the wrappedStatement and assign it to QueryDetails Object
     *
     * @param wrappedStatement
     * @param queryBasicInfo
     * @return QueryDetails
     */
    private QueryDetails EnrichQueryTableDetailsFromWrappedStatement(Statement wrappedStatement, QueryBasicInfo queryBasicInfo)
    {
        QueryDetails queryDetails = new QueryDetails();
        Map<String, TableDetails> queryTableDetails = new HashMap<>();
        Map<String, TableBasicInfo> tableBasicInfoMap = queryBasicInfo.getTableBasicInfo();

        if (null != wrappedStatement && (wrappedStatement instanceof Query)) {
            Query query = (Query) wrappedStatement;
            if (null != query && null != query.getQueryBody() && query.getQueryBody() instanceof QuerySpecification) {
                QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();

                //select * scenario queryBasicInfo.isAllColumnQuery() This value will be already set based on the condition
                queryDetails.setAllColumnQuery(queryBasicInfo.isAllColumnQuery());

                Optional<Relation> fromTableData = querySpecification.getFrom();
                if (fromTableData.isPresent()) {
                    if (fromTableData.get() instanceof Table) {
                        Table tableData = (Table) fromTableData.get();
                        String strTableName = getEntityNameFromTableObject(tableData, TABLE_NAME);
                        String strSchemaName = getEntityFromTableBasicInfo(tableBasicInfoMap, strTableName, SCHEMA_NAME);
                        String strCatalogName = getEntityFromTableBasicInfo(tableBasicInfoMap, strTableName, CATALOG_NAME);
                        //String strSchemaName = getEntityNameFromTableObject(tableData, SCHEMA_NAME);
                        //String strCatalogName = getEntityNameFromTableObject(tableData, CATALOG_NAME);
                        TableDetails mainTableDetails = new TableDetails();
                        mainTableDetails.setTableName(strTableName);
                        mainTableDetails.setSchemaName(strSchemaName);
                        mainTableDetails.setCatalogName(strCatalogName);
                        mainTableDetails.setAvailableColumnList(getAvailableColumnListFromTableBasicInfo(tableBasicInfoMap, strTableName));
                        mainTableDetails.setColumnMetadataUnavailable(tableBasicInfoMap.get(strTableName).isColumnMetadataUnavailable());
                        queryTableDetails.put(strTableName, mainTableDetails);
                        queryDetails.setQueryType(SIMPLE);
                    }
                    else if (fromTableData.get() instanceof Join) {
                        Join joinTableData = (Join) fromTableData.get();
                        String joinType = joinTableData.getType().name();
                        if (IMPLICIT.equalsIgnoreCase(joinType) || INNER.equalsIgnoreCase(joinType) || LEFT.equalsIgnoreCase(joinType) || RIGHT.equalsIgnoreCase(joinType) || FULL.equalsIgnoreCase(joinType)) {
                            if (joinTableData.getLeft() instanceof Table && joinTableData.getRight() instanceof Table) {
                                Table leftTableData = (Table) joinTableData.getLeft();
                                Table rightTableData = (Table) joinTableData.getRight();

                                String leftTableName = getEntityNameFromTableObject(leftTableData, TABLE_NAME);
                                String leftTableSchemaName = getEntityFromTableBasicInfo(tableBasicInfoMap, leftTableName, SCHEMA_NAME);
                                String leftTableCatalogName = getEntityFromTableBasicInfo(tableBasicInfoMap, leftTableName, CATALOG_NAME);
                                //String leftTableSchemaName = getEntityNameFromTableObject(leftTableData, SCHEMA_NAME);
                                //String leftTableCatalogName = getEntityNameFromTableObject(leftTableData, CATALOG_NAME);

                                String rightTableName = getEntityNameFromTableObject(rightTableData, TABLE_NAME);
                                String rightTableSchemaName = getEntityFromTableBasicInfo(tableBasicInfoMap, rightTableName, SCHEMA_NAME);
                                String rightTableCatalogName = getEntityFromTableBasicInfo(tableBasicInfoMap, rightTableName, CATALOG_NAME);
                                //String rightTableSchemaName = getEntityNameFromTableObject(rightTableData, SCHEMA_NAME);
                                //String rightTableCatalogName = getEntityNameFromTableObject(rightTableData, CATALOG_NAME);

                                //String leftTableName = getTableNameFromTableObject((Table) joinTableData.getLeft());
                                //String rightTableName = getTableNameFromTableObject((Table) joinTableData.getRight());

                                TableDetails leftTableDetails = new TableDetails();
                                leftTableDetails.setTableName(leftTableName);
                                leftTableDetails.setSchemaName(leftTableSchemaName);
                                leftTableDetails.setCatalogName(leftTableCatalogName);
                                leftTableDetails.setAvailableColumnList(getAvailableColumnListFromTableBasicInfo(tableBasicInfoMap, leftTableName));
                                leftTableDetails.setColumnMetadataUnavailable(tableBasicInfoMap.get(leftTableName).isColumnMetadataUnavailable());
                                queryTableDetails.put(leftTableName, leftTableDetails);

                                TableDetails rightTableDetails = new TableDetails();
                                rightTableDetails.setTableName(rightTableName);
                                rightTableDetails.setSchemaName(rightTableSchemaName);
                                rightTableDetails.setCatalogName(rightTableCatalogName);
                                rightTableDetails.setAvailableColumnList(getAvailableColumnListFromTableBasicInfo(tableBasicInfoMap, rightTableName));
                                rightTableDetails.setColumnMetadataUnavailable(tableBasicInfoMap.get(rightTableName).isColumnMetadataUnavailable());
                                queryTableDetails.put(rightTableName, rightTableDetails);
                                if (IMPLICIT.equalsIgnoreCase(joinType)) {
                                    queryDetails.setQueryType(IMPLICIT_JOIN);
                                }
                                else if (INNER.equalsIgnoreCase(joinType)) {
                                    queryDetails.setQueryType(INNER_JOIN);
                                }
                                else if (LEFT.equalsIgnoreCase(joinType)) {
                                    queryDetails.setQueryType(LEFT_JOIN);
                                }
                                else if (RIGHT.equalsIgnoreCase(joinType)) {
                                    queryDetails.setQueryType(RIGHT_JOIN);
                                }
                                else if (FULL.equalsIgnoreCase(joinType)) {
                                    queryDetails.setQueryType(FULL_OUTER_JOIN);
                                }
                            }
                        }
                    }
                }
                queryDetails.setQueryTableDetails(queryTableDetails);
            }
        }
        return queryDetails;
    }

    private String getEntityFromTableBasicInfo(Map<String, TableBasicInfo> tableBasicInfoMap, String strTableName, String entity)
    {
        TableBasicInfo tableBasicInfo = tableBasicInfoMap.get(strTableName);
        switch (entity) {
            case SCHEMA_NAME:
                return tableBasicInfo.getSchemaName();
            case CATALOG_NAME:
                return tableBasicInfo.getCatalogName();
        }
        return null;
    }

    private List<String> getAvailableColumnListFromTableBasicInfo(Map<String, TableBasicInfo> tableBasicInfoMap, String strTableName)
    {
        if (null != tableBasicInfoMap && !tableBasicInfoMap.isEmpty() && tableBasicInfoMap.containsKey(strTableName)) {
            return tableBasicInfoMap.get(strTableName).getAvailableColumnList();
        }
        return null;
    }

    /**
     * This method will return the table name from the Table Object
     *
     * @param tableData : Table Object
     * @return tableName
     */
    private String getTableNameFromTableObject(Table tableData)
    {
        String tableName = null;
        QualifiedName name = tableData.getName();
        if (null != name && null != name.getParts() && !name.getParts().isEmpty()) {
            for (String tableDetails : name.getParts()) {
                tableName = tableDetails;
            }
        }
        return tableName;
    }

    /**
     * This method will return QueryBasicInfo
     *
     * @param preparedQuery
     * @param catalog
     * @param schema
     * @param identity
     * @return
     */
    public QueryBasicInfo getQueryBasicInfo(PreparedQuery preparedQuery, String catalog, String schema, Identity identity)
    {
        QueryBasicInfo queryBasicInfo = getBasicQueryTableSchemaDetailsFromWrappedStatement(((BuiltInQueryPreparer.BuiltInPreparedQuery) preparedQuery).getWrappedStatement());

        if (null != identity.getUser()) {
            queryBasicInfo.setUserName(identity.getUser());
        }
        if (null != queryBasicInfo && null != queryBasicInfo.getTableBasicInfo() && !queryBasicInfo.getTableBasicInfo().isEmpty()) {
            Map<String, TableBasicInfo> tableBasicInfo = queryBasicInfo.getTableBasicInfo();
            tableBasicInfo.entrySet().stream().forEach(entry -> {
                if (null != entry.getValue()) {
                    TableBasicInfo tableBasicInfoObj = entry.getValue();
                    if (StringHelperUtil.isNullString(tableBasicInfoObj.getCatalogName())) {
                        tableBasicInfoObj.setCatalogName(catalog);
                    }
                    if (StringHelperUtil.isNullString(tableBasicInfoObj.getSchemaName())) {
                        tableBasicInfoObj.setSchemaName(schema);
                    }
                }
            });
        }

        return queryBasicInfo;
    }

    /**
     * This method will return the Basic Query Details from the Query Object
     *
     * @param wrappedStatement : Staatement Query Object
     * @return QueryBasicInfo
     */
    public QueryBasicInfo getBasicQueryTableSchemaDetailsFromWrappedStatement(Statement wrappedStatement)
    {
        QueryBasicInfo queryBasicDetails = new QueryBasicInfo();
        Map<String, TableBasicInfo> queryTableBasicDetails = new HashMap<>();

        if (null != wrappedStatement && (wrappedStatement instanceof Query)) {
            Query query = (Query) wrappedStatement;
            if (null != query && null != query.getQueryBody() && query.getQueryBody() instanceof QuerySpecification) {
                QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();

                List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();
                // select * scenario
                if (selectItems.stream().anyMatch(AllColumns.class::isInstance)) {
                    queryBasicDetails.setAllColumnQuery(true);
                }

                Optional<Relation> fromTableData = querySpecification.getFrom();
                if (fromTableData.isPresent()) {
                    if (fromTableData.get() instanceof Table) {
                        Table tableData = (Table) fromTableData.get();
                        String strTableName = getEntityNameFromTableObject(tableData, TABLE_NAME);
                        String strSchemaName = getEntityNameFromTableObject(tableData, SCHEMA_NAME);
                        String strCatalogName = getEntityNameFromTableObject(tableData, CATALOG_NAME);
                        TableBasicInfo mainTableDetails = new TableBasicInfo();
                        mainTableDetails.setTableName(strTableName);
                        mainTableDetails.setSchemaName(strSchemaName);
                        mainTableDetails.setCatalogName(strCatalogName);
                        queryTableBasicDetails.put(strTableName, mainTableDetails);
                    }
                    else if (fromTableData.get() instanceof Join) {
                        Join joinTableData = (Join) fromTableData.get();
                        String joinType = joinTableData.getType().name();
                        if (IMPLICIT.equalsIgnoreCase(joinType) || INNER.equalsIgnoreCase(joinType) || LEFT.equalsIgnoreCase(joinType) || RIGHT.equalsIgnoreCase(joinType) || FULL.equalsIgnoreCase(joinType)) {
                            if (joinTableData.getLeft() instanceof Table && joinTableData.getRight() instanceof Table) {
                                String leftTableName = getEntityNameFromTableObject((Table) joinTableData.getLeft(), TABLE_NAME);
                                String leftSchemaName = getEntityNameFromTableObject((Table) joinTableData.getLeft(), SCHEMA_NAME);
                                String leftCatalogName = getEntityNameFromTableObject((Table) joinTableData.getLeft(), CATALOG_NAME);

                                String rightTableName = getEntityNameFromTableObject((Table) joinTableData.getRight(), TABLE_NAME);
                                String rightSchemaName = getEntityNameFromTableObject((Table) joinTableData.getRight(), SCHEMA_NAME);
                                String rightCatalogName = getEntityNameFromTableObject((Table) joinTableData.getRight(), CATALOG_NAME);

                                TableBasicInfo leftTableDetails = new TableBasicInfo();
                                leftTableDetails.setTableName(leftTableName);
                                leftTableDetails.setSchemaName(leftSchemaName);
                                leftTableDetails.setCatalogName(leftCatalogName);
                                queryTableBasicDetails.put(leftTableName, leftTableDetails);

                                TableBasicInfo rightTableDetails = new TableBasicInfo();
                                rightTableDetails.setTableName(rightTableName);
                                rightTableDetails.setSchemaName(rightSchemaName);
                                rightTableDetails.setCatalogName(rightCatalogName);
                                queryTableBasicDetails.put(rightTableName, rightTableDetails);
                            }
                        }
                    }
                }
                queryBasicDetails.setTableBasicInfo(queryTableBasicDetails);

                Optional<Expression> whereClause = querySpecification.getWhere();
                Optional<OrderBy> orderByClause = querySpecification.getOrderBy();
                Optional<GroupBy> groupByClause = querySpecification.getGroupBy();
                Optional<Expression> havingClause = querySpecification.getHaving();

                if (whereClause.isPresent() && null != whereClause.get()) {
                    queryBasicDetails.setWhereClausePresent(true);
                }
                if (orderByClause.isPresent() && null != orderByClause.get()) {
                    queryBasicDetails.setOrderByClausePresent(true);
                }
                if (groupByClause.isPresent() && null != groupByClause.get()) {
                    queryBasicDetails.setGroupByClausePresent(true);
                }
                if (havingClause.isPresent() && null != havingClause.get()) {
                    queryBasicDetails.setHavingClausePresent(true);
                }
            }
        }
        return queryBasicDetails;
    }

    /**
     * This method will return the table name from the Table Object
     *
     * @param tableData : Table Object
     * @return tableName
     */
    private String getEntityNameFromTableObject(Table tableData, String entity)
    {
        String tableName = null;
        String schemaName = null;
        String catalogName = null;
        QualifiedName name = tableData.getName();
        if (null != name && null != name.getParts() && !name.getParts().isEmpty()) {
            int size = name.getParts().size();
            switch (size) {
                case 1:
                    tableName = name.getParts().get(0);
                    break;
                case 2:
                    schemaName = name.getParts().get(0);
                    tableName = name.getParts().get(1);
                    break;
                case 3:
                    catalogName = name.getParts().get(0);
                    schemaName = name.getParts().get(1);
                    tableName = name.getParts().get(2);
                    break;
            }
        }
        if (TABLE_NAME.equalsIgnoreCase(entity)) {
            return tableName;
        }
        else if (SCHEMA_NAME.equalsIgnoreCase(entity)) {
            return schemaName;
        }
        else if (CATALOG_NAME.equalsIgnoreCase(entity)) {
            return catalogName;
        }
        return null;
    }

    public String enrichSelectAllColumnQuery(QueryRewriteApplicableResponse queryRewriteApplicableResponse, String originalQuery)
    {
        QueryDetails queryDetails = queryRewriteApplicableResponse.getQueryDetails();
        if (!queryDetails.isAllColumnQuery()) {
            return originalQuery;
        }

        String updatedQuery = originalQuery;
        updatedQuery = updatedQuery.replaceAll(DOUBLE_QUOTES_SYMBOL, EMPTY_STRING);

        Map<String, TableDetails> queryTableDetailsMap = queryDetails.getQueryTableDetails();
        String replaceString = EMPTY_STRING;

        for (Map.Entry<String, TableDetails> entry : queryTableDetailsMap.entrySet()) {
            TableDetails tableDetValue = entry.getValue();
            String strCatalogName = tableDetValue.getCatalogName();
            String strSchemaName = tableDetValue.getSchemaName();
            String strTableName = tableDetValue.getTableName();
            List<String> availableColumnList = tableDetValue.getAvailableColumnList();
            if (null != availableColumnList && !availableColumnList.isEmpty()) {
                for (String strColumnName : availableColumnList) {
                    replaceString = replaceString + COMMA_SYMBOL + DOUBLE_QUOTES_SYMBOL + strCatalogName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strSchemaName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strTableName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + strColumnName;
                }
            }

            //To Handle the Scenario if the Column Details MetaData is not available
            if (tableDetValue.isColumnMetadataUnavailable()) {
                replaceString = replaceString + COMMA_SYMBOL + DOUBLE_QUOTES_SYMBOL + strCatalogName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strSchemaName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strTableName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + STAR_SYMBOL;
            }

            if (updatedQuery.contains(strCatalogName + DOT_SYMBOL + strSchemaName + DOT_SYMBOL + strTableName)) {
                updatedQuery = updatedQuery.replaceAll(strCatalogName + DOT_SYMBOL + strSchemaName + DOT_SYMBOL + strTableName, DOUBLE_QUOTES_SYMBOL + strCatalogName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strSchemaName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strTableName + DOUBLE_QUOTES_SYMBOL);
            }
            else if (updatedQuery.contains(strSchemaName + DOT_SYMBOL + strTableName)) {
                updatedQuery = updatedQuery.replaceAll(strSchemaName + DOT_SYMBOL + strTableName, DOUBLE_QUOTES_SYMBOL + strCatalogName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strSchemaName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strTableName + DOUBLE_QUOTES_SYMBOL);
            }
            else if (updatedQuery.contains(strTableName)) {
                updatedQuery = updatedQuery.replaceAll(strTableName, DOUBLE_QUOTES_SYMBOL + strCatalogName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strSchemaName + DOUBLE_QUOTES_SYMBOL + DOT_SYMBOL + DOUBLE_QUOTES_SYMBOL + strTableName + DOUBLE_QUOTES_SYMBOL);
            }
        }
        replaceString = replaceString.replaceFirst(COMMA_SYMBOL, EMPTY_STRING);

        updatedQuery = updatedQuery.replace(STAR_SYMBOL, replaceString);

        return updatedQuery;
    }
}
