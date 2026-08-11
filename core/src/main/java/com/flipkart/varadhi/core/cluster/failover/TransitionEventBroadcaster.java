package com.flipkart.varadhi.core.cluster.failover;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import lombok.experimental.UtilityClass;

/**
 * Controller-side binder for topic-transition stage broadcasts.
 *
 * <p>Pods never call this; they subscribe via {@link TransitionEventSubscriber}. The controller
 * orchestrator ({@code TransitionService}) publishes through here so call sites never pass string
 * route/api pairs.
 */
@UtilityClass
public final class TransitionEventBroadcaster {

    /**
     * Publishes a stage event to all subscribed pods.
     */
    public static void publish(MessageExchange exchange, TransitionEvent event) {
        exchange.publish(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.EVENT_PUBLISH_API,
            ClusterMessage.of(event)
        );
    }
}
