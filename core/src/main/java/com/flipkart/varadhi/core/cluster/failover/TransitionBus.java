package com.flipkart.varadhi.core.cluster.failover;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import lombok.experimental.UtilityClass;

/**
 * Typed binder for the topic-transition publish bus.
 *
 * <p>{@link MessageRouter#registerPublishReceiveHandler} stays transport plumbing; call sites use
 * {@link #subscribe} / {@link #publish} so they never pass string route/api pairs.
 */
@UtilityClass
public final class TransitionBus {

    /**
     * Registers {@code listener} for controller → pod stage broadcasts.
     */
    public static void subscribe(MessageRouter router, TransitionEventListener listener) {
        router.registerPublishReceiveHandler(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.EVENT_PUBLISH_API,
            message -> listener.onTransition(message.getData(TransitionEvent.class))
        );
    }

    /**
     * Publishes a stage event to all subscribed pods (controller-local).
     */
    public static void publish(MessageExchange exchange, TransitionEvent event) {
        exchange.publish(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.EVENT_PUBLISH_API,
            ClusterMessage.of(event)
        );
    }
}
