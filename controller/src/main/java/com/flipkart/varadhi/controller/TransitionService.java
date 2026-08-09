package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.controller.TransitionAckApi;
import com.flipkart.varadhi.core.cluster.controller.TransitionPublisher;
import com.flipkart.varadhi.core.cluster.failover.TransitionBus;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * Controller-local topic-transition service: broadcast stage events and accept pod acks.
 */
@Slf4j
public class TransitionService implements TransitionPublisher, TransitionAckApi {

    private final MessageExchange messageExchange;

    public TransitionService(MessageExchange messageExchange) {
        this.messageExchange = messageExchange;
    }

    @Override
    public CompletableFuture<Void> broadcastEvent(TransitionEvent event) {
        try {
            TransitionBus.publish(messageExchange, event);
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
