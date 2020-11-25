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

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.util.Optional;
import java.util.UUID;

public final class QueryEventListener
        implements EventListener
{
    private final Logger logger;
    private final boolean trackEventCreated;
    private final boolean trackEventCompleted;
    private final boolean trackEventCompletedSplit;

    private final String instanceId;
    private WebSocketCollectorChannel webSocketCollectorChannel;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String clusterName;
    private boolean sendToWebSocketServer;

    public QueryEventListener(String clusterName,
            final LoggerContext loggerContext,
            final boolean sendToWebSocketServer,
            String webSockerCollectUrl,
            final boolean trackEventCreated,
            final boolean trackEventCompleted,
            final boolean trackEventCompletedSplit)
    {
        this.instanceId = UUID.randomUUID().toString();
        this.clusterName = clusterName;
        this.trackEventCreated = trackEventCreated;
        this.trackEventCompleted = trackEventCompleted;
        this.trackEventCompletedSplit = trackEventCompletedSplit;
        this.logger = loggerContext.getLogger(QueryEventListener.class.getName());

        if (sendToWebSocketServer) {
            this.webSocketCollectorChannel = new WebSocketCollectorChannel(webSockerCollectUrl);
            this.webSocketCollectorChannel.connect();
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (!trackEventCreated) {
            return;
        }

        String query = queryCreatedEvent.getMetadata().getQuery();
        QueryMetadata queryMetadata = new QueryMetadata(
                queryCreatedEvent.getMetadata().getQueryId(),
                queryCreatedEvent.getMetadata().getTransactionId(),
                flattenQuery(query),
                queryCreatedEvent.getMetadata().getQueryState(),
                queryCreatedEvent.getMetadata().getUri(),
                queryCreatedEvent.getMetadata().getPlan(),
                queryCreatedEvent.getMetadata().getJsonPlan(),
                queryCreatedEvent.getMetadata().getPayload(),
                queryCreatedEvent.getMetadata().getRuntimeOptimizedStages());

        QueryCreatedEvent queryCreatedEvent1 = new QueryCreatedEvent(
                queryCreatedEvent.getCreateTime(),
                queryCreatedEvent.getContext(),
                queryMetadata);

        try {
            String eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, queryCreatedEvent1, null, null));
            logger.info(eventPayload);
            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
            }
        }
        catch (JsonProcessingException e) {
            logger.warn("Failed to serialize query log event", e);
        }
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (!trackEventCompleted) {
            return;
        }

        String query = queryCompletedEvent.getMetadata().getQuery();
        QueryMetadata queryMetadata = new QueryMetadata(
                queryCompletedEvent.getMetadata().getQueryId(),
                queryCompletedEvent.getMetadata().getTransactionId(),
                flattenQuery(query),
                queryCompletedEvent.getMetadata().getQueryState(),
                queryCompletedEvent.getMetadata().getUri(),
                queryCompletedEvent.getMetadata().getPlan(),
                queryCompletedEvent.getMetadata().getJsonPlan(),
                queryCompletedEvent.getMetadata().getPayload(),
                queryCompletedEvent.getMetadata().getRuntimeOptimizedStages());

        QueryCompletedEvent queryCompletedEvent1 = new QueryCompletedEvent(
                queryMetadata,
                queryCompletedEvent.getStatistics(),
                queryCompletedEvent.getContext(),
                queryCompletedEvent.getIoMetadata(),
                queryCompletedEvent.getFailureInfo(),
                queryCompletedEvent.getWarnings(),
                queryCompletedEvent.getQueryType(),
                queryCompletedEvent.getFailedTasks(),
                queryCompletedEvent.getCreateTime(),
                queryCompletedEvent.getExecutionStartTime(),
                queryCompletedEvent.getEndTime(),
                queryCompletedEvent.getStageStatistics(),
                queryCompletedEvent.getOperatorStatistics());

        try {
            String eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, null, queryCompletedEvent1, null));
            logger.info(eventPayload);
            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
            }
        }
        catch (JsonProcessingException e) {
            logger.warn("Failed to serialize query log event", e);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (!trackEventCompletedSplit) {
            return;
        }

        try {
            String eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, null, null, splitCompletedEvent));
            logger.info(eventPayload);
            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
            }
        }
        catch (JsonProcessingException e) {
            logger.warn("Failed to serialize query log event", e);
        }
    }

    private static String flattenQuery(String query)
    {
        return (Optional.ofNullable(query).isPresent())
                ? query.replaceAll("\n", "") : "";
    }
}
