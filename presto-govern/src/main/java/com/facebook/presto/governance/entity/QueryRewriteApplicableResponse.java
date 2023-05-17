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

import java.util.List;

public class QueryRewriteApplicableResponse
{
    public boolean rewriteApplicable;

    public List<GovernPolicyFormat> applicablePolicies;

    public QueryDetails queryDetails;

    public boolean isRewriteApplicable()
    {
        return rewriteApplicable;
    }

    public void setRewriteApplicable(boolean rewriteApplicable)
    {
        this.rewriteApplicable = rewriteApplicable;
    }

    public List<GovernPolicyFormat> getApplicablePolicies()
    {
        return applicablePolicies;
    }

    public void setApplicablePolicies(List<GovernPolicyFormat> applicablePolicies)
    {
        this.applicablePolicies = applicablePolicies;
    }

    public QueryDetails getQueryDetails()
    {
        return queryDetails;
    }

    public void setQueryDetails(QueryDetails queryDetails)
    {
        this.queryDetails = queryDetails;
    }
}
