package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.controller.TransitionApi;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * Controller-local topic-transition service: broadcast stage events and accept pod acks.
 */
@Slf4j
public class TransitionService implements TransitionApi {

    private final MessageExchange messageExchange;

    public TransitionService(MessageExchange messageExchange) {
        this.messageExchange = messageExchange;
    }

    @Override
    public CompletableFuture<Void> sendEvent(TransitionEvent event) {
        try {
            messageExchange.publish(
                TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
                TransitionBusAddress.EVENT_PUBLISH_API,
                ClusterMessage.of(event)
            );
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> ack(TransitionAck ack) {
        // Delivery is accepted here; stage-barrier orchestration will consume these acks when wired.
        log.debug("Received topic-transition ack: {}", ack);
        return CompletableFuture.completedFuture(null);
    }
}
