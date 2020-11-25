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
package io.ahana.eventplugin;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;

public final class QueryEvent
{
    private final String instanceId;
    private final String clusterName;
    private final QueryCreatedEvent queryCreatedEvent;
    private final QueryCompletedEvent queryCompletedEvent;
    private final SplitCompletedEvent splitCompletedEvent;

    public QueryEvent(
            String instanceId,
            String clusterName,
            QueryCreatedEvent queryCreatedEvent,
            QueryCompletedEvent queryCompletedEvent,
            SplitCompletedEvent splitCompletedEvent)
    {
        this.instanceId = instanceId;
        this.clusterName = clusterName;
        this.queryCreatedEvent = queryCreatedEvent;
        this.queryCompletedEvent = queryCompletedEvent;
        this.splitCompletedEvent = splitCompletedEvent;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public QueryCreatedEvent getQueryCreatedEvent()
    {
        return queryCreatedEvent;
    }

    public QueryCompletedEvent getQueryCompletedEvent()

    {
        return queryCompletedEvent;
    }

    public SplitCompletedEvent getSplitCompletedEvent()
    {
        return splitCompletedEvent;
    }
}
