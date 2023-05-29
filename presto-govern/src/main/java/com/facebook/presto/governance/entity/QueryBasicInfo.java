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

import java.util.Map;

public class QueryBasicInfo
{
    String userName;
    Map<String, TableBasicInfo> tableBasicInfo;

    boolean allColumnQuery;

    boolean whereClausePresent;

    boolean orderByClausePresent;

    boolean groupByClausePresent;

    boolean havingClausePresent;

    public String getUserName()
    {
        return userName;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public Map<String, TableBasicInfo> getTableBasicInfo()
    {
        return tableBasicInfo;
    }

    public void setTableBasicInfo(Map<String, TableBasicInfo> tableBasicInfo)
    {
        this.tableBasicInfo = tableBasicInfo;
    }

    public boolean isAllColumnQuery()
    {
        return allColumnQuery;
    }

    public void setAllColumnQuery(boolean allColumnQuery)
    {
        this.allColumnQuery = allColumnQuery;
    }

    public boolean isWhereClausePresent()
    {
        return whereClausePresent;
    }

    public void setWhereClausePresent(boolean whereClausePresent)
    {
        this.whereClausePresent = whereClausePresent;
    }

    public boolean isOrderByClausePresent()
    {
        return orderByClausePresent;
    }

    public void setOrderByClausePresent(boolean orderByClausePresent)
    {
        this.orderByClausePresent = orderByClausePresent;
    }

    public boolean isGroupByClausePresent()
    {
        return groupByClausePresent;
    }

    public void setGroupByClausePresent(boolean groupByClausePresent)
    {
        this.groupByClausePresent = groupByClausePresent;
    }

    public boolean isHavingClausePresent()
    {
        return havingClausePresent;
    }

    public void setHavingClausePresent(boolean havingClausePresent)
    {
        this.havingClausePresent = havingClausePresent;
    }
}
