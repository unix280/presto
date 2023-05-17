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
package com.facebook.presto.governance.entity;

import java.util.ArrayList;
import java.util.Map;

public class GovernPolicyFormat
{
    public String tableName;

    public ArrayList<String> columns;
    public Map<String, UserDefinedFunctionInfo> userDefinedFunctionInfo;

    public String schemaName;

    public String catalogName;

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public ArrayList<String> getColumns()
    {
        return columns;
    }

    public void setColumns(ArrayList<String> columns)
    {
        this.columns = columns;
    }

    public Map<String, UserDefinedFunctionInfo> getUserDefinedFunctionInfo()
    {
        return userDefinedFunctionInfo;
    }

    public void setUserDefinedFunctionInfo(Map<String, UserDefinedFunctionInfo> userDefinedFunctionInfo)
    {
        this.userDefinedFunctionInfo = userDefinedFunctionInfo;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }
}
