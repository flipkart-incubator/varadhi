package com.flipkart.varadhi.core.cluster.failover;

import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import lombok.experimental.UtilityClass;

/**
 * Pod-side binder for topic-transition stage broadcasts.
 *
 * <p>{@link MessageRouter#registerPublishReceiveHandler} stays transport plumbing; pods subscribe
 * here so they never pass string route/api pairs. Controller publish is
 * {@link TransitionEventBroadcaster}.
 */
@UtilityClass
public final class TransitionEventSubscriber {

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
}
