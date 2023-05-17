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

public class QueryDetails
{
    public String queryType;

    Map<String, TableDetails> queryTableDetails;

    boolean allColumnQuery;

    public String getQueryType()
    {
        return queryType;
    }

    public void setQueryType(String queryType)
    {
        this.queryType = queryType;
    }

    public Map<String, TableDetails> getQueryTableDetails()
    {
        return queryTableDetails;
    }

    public void setQueryTableDetails(Map<String, TableDetails> queryTableDetails)
    {
        this.queryTableDetails = queryTableDetails;
    }

    public boolean isAllColumnQuery()
    {
        return allColumnQuery;
    }

    public void setAllColumnQuery(boolean allColumnQuery)
    {
        this.allColumnQuery = allColumnQuery;
    }
}
