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
package io.ahana.juicyray;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class Listener
        implements EventListener
{
    private final String instanceId;
    private final CollectorChannel collectorChannel;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicLong seq = new AtomicLong(0L);
    private final String clusterName;
    private static final Logger log = Logger.get(Listener.class);

    public Listener(Map<String, String> config)
    {
        this.instanceId = UUID.randomUUID().toString();
        this.mapper.setSerializationInclusion(Include.NON_NULL);

        this.clusterName =
                Optional.ofNullable(
                        (String)
                                config.getOrDefault("cluster-name", System.getenv("JUICYRAY_CLUSTER_NAME")))
                        .orElseThrow(IllegalArgumentException::new);

        String url =
                Optional.ofNullable(
                        (String) config.getOrDefault("url", System.getenv("JUICYRAY_COLLECTOR_URL")))
                        .orElseThrow(IllegalArgumentException::new);

        this.collectorChannel = new CollectorChannel(url);
        this.collectorChannel.connect();
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        try {
            this.collectorChannel.sendMessage(
                    this.mapper.writeValueAsString(
                            new Event(
                                    "queryCreated",
                                    this.instanceId,
                                    this.clusterName,
                                    this.seq.incrementAndGet(),
                                    System.currentTimeMillis() / 1000.0,
                                    queryCreatedEvent,
                                    null,
                                    null)));
        }
        catch (JsonProcessingException e) {
            log.warn(e, "Failed to serialize query log event");
        }
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        try {
            this.collectorChannel.sendMessage(
                    this.mapper.writeValueAsString(
                            new Event(
                                    "queryCompleted",
                                    this.instanceId,
                                    this.clusterName,
                                    this.seq.incrementAndGet(),
                                    System.currentTimeMillis() / 1000.0,
                                    null,
                                    queryCompletedEvent,
                                    null)));
        }
        catch (JsonProcessingException e) {
            log.warn(e, "Failed to serialize query log event");
        }
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        try {
            this.collectorChannel.sendMessage(
                    this.mapper.writeValueAsString(
                            new Event(
                                    "splitCompleted",
                                    this.instanceId,
                                    this.clusterName,
                                    this.seq.incrementAndGet(),
                                    System.currentTimeMillis() / 1000.0,
                                    null,
                                    null,
                                    splitCompletedEvent)));
        }
        catch (JsonProcessingException e) {
            log.warn(e, "Failed to serialize query log event");
        }
    }
}
