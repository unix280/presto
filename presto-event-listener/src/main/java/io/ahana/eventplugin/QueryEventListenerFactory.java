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
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class QueryEventListenerFactory
        implements EventListenerFactory
{
    public static final String QUERYEVENT_CLUSTER_NAME = "juicyray.queryevent.clustername";

    public static final String QUERYEVENT_CONFIG_LOCATION = "juicyray.queryevent.log4j2.configLocation";
    public static final String QUERYEVENT_TRACK_CREATED = "juicyray.queryevent.log.queryCreatedEvent";
    public static final String QUERYEVENT_TRACK_COMPLETED = "juicyray.queryevent.log.queryCompletedEvent";
    public static final String QUERYEVENT_TRACK_COMPLETED_SPLIT = "juicyray.queryevent.log.splitCompletedEvent";

    public static final String QUERYEVENT_WEBSOCKET_COLLECT = "juicyray.queryevent.websocket.enabled";
    public static final String QUERYEVENT_WEBSOCKET_URL = "juicyray.queryevent.websocket.url";

    private static final String QUERYEVENT_CONFIG_LOCATION_ERROR = QUERYEVENT_CONFIG_LOCATION + " is null";
    private static final String QUERYEVENT_CLUSTER_NAME_ERROR = QUERYEVENT_CLUSTER_NAME + " is null";
    private static final String QUERYEVENT_WEBSOCKET_URL_ERROR = QUERYEVENT_WEBSOCKET_URL + " is null";

    public String getName()
    {
        return "ahana-events";
    }

    public EventListener create(Map<String, String> config)
    {
        String log4j2ConfigLocation = requireNonNull(config.get(QUERYEVENT_CONFIG_LOCATION), QUERYEVENT_CONFIG_LOCATION_ERROR);
        String clusterName = requireNonNull(config.get(QUERYEVENT_CLUSTER_NAME), QUERYEVENT_CLUSTER_NAME_ERROR);
        String webSocketCollectUrl = null;
        boolean sendToWebsockeCollector = getBooleanConfig(config, QUERYEVENT_WEBSOCKET_COLLECT, false);
        if (sendToWebsockeCollector) {
            webSocketCollectUrl = requireNonNull(config.get(QUERYEVENT_WEBSOCKET_URL), QUERYEVENT_WEBSOCKET_URL_ERROR);
        }

        LoggerContext loggerContext = Configurator.initialize("presto-queryevent-log", log4j2ConfigLocation);
        boolean trackEventCreated = getBooleanConfig(config, QUERYEVENT_TRACK_CREATED, true);
        boolean trackEventCompleted = getBooleanConfig(config, QUERYEVENT_TRACK_COMPLETED, true);
        boolean trackEventCompletedSplit = getBooleanConfig(config, QUERYEVENT_TRACK_COMPLETED_SPLIT, false);
        return new QueryEventListener(clusterName, loggerContext, sendToWebsockeCollector, webSocketCollectUrl, trackEventCreated, trackEventCompleted, trackEventCompletedSplit);
    }

    /**
     * Get {@code boolean} parameter value, or return default.
     *
     * @param params Map of parameters
     * @param paramName Parameter name
     * @param paramDefault Parameter default value
     * @return Parameter value or default.
     */
    private boolean getBooleanConfig(Map<String, String> params, String paramName, boolean paramDefault)
    {
        String value = params.get(paramName);
        if (value != null && !value.trim().isEmpty()) {
            return Boolean.parseBoolean(value);
        }
        return paramDefault;
    }
}
