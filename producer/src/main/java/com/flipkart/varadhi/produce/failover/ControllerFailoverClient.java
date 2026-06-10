package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate;
import lombok.extern.slf4j.Slf4j;

/**
 * Thin pod-side client that sends an ack ({@link FailoverStatusUpdate}) back to the
 * controller after a stage trigger has been processed locally.
 *
 * <p>Uses {@code MessageExchange.send} (fire-and-forget on the wire) addressed to
 * {@code controller.failoverAck.send}; the controller side wires the matching
 * {@code sendHandler} in {@code ControllerVerticle.setupApiHandlers}.
 */
@Slf4j
public final class ControllerFailoverClient {

    /** Matches the address registered by {@code ControllerApiHandler::failoverAck}. */
    public static final String API_FAILOVER_ACK = "failoverAck";

    private final MessageExchange exchange;

    public ControllerFailoverClient(MessageExchange exchange) {
        this.exchange = exchange;
    }

    public void ack(FailoverStatusUpdate update) {
        exchange.send(ControllerApi.ROUTE_CONTROLLER, API_FAILOVER_ACK, ClusterMessage.of(update))
                .exceptionally(t -> {
                    log.warn("Failed to send failover ack {}: {}", update, t.getMessage());
                    return null;
                });
    }
}
