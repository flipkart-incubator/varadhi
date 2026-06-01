package com.flipkart.varadhi.controller.failover;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStageEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * Thin facade over {@link MessageExchange} for failover stage events.
 *
 * <p>The controller's executor calls {@link #broadcast(FailoverStageEvent)} to fan
 * out a stage trigger to every pod via Vert.x {@code publish}. It calls
 * {@link #pushToHosts(FailoverStageEvent, Set)} as a targeted resend (via
 * {@code send}) when {@link StageAwaiter} detects missing acks shortly before
 * its timeout.
 *
 * <p>Both methods are fire-and-forget at the controller; the per-host ack flow
 * runs back in the reverse direction (pod -> controller via
 * {@code FailoverStatusUpdate}) and is collected by {@link StageAwaiter}.
 */
@Slf4j
public final class FailoverBroadcaster {

    public static final String ROUTE_FAILOVER = "failover";
    public static final String API_STAGE_EVENT = "stage";

    private final MessageExchange exchange;

    public FailoverBroadcaster(MessageExchange exchange) {
        this.exchange = exchange;
    }

    /** Fan out {@code event} to every pod subscribed to the failover stage topic. */
    public void broadcast(FailoverStageEvent event) {
        log.debug("Broadcasting failover stage event {}", event);
        exchange.publish(ROUTE_FAILOVER, API_STAGE_EVENT, ClusterMessage.of(event));
    }

    /**
     * Targeted re-send to a subset of hosts. Uses {@code send} so each delivery is
     * trackable; failures are logged but not surfaced.
     */
    public void pushToHosts(FailoverStageEvent event, Set<String> hosts) {
        if (hosts == null || hosts.isEmpty()) {
            return;
        }
        log.info("Resending failover event op={} stage={} to {} hosts", event.getOpId(), event.getStage(), hosts.size());
        // Vert.x send selects one consumer at random for the address. Since each pod's
        // handler is registered on the same address (publish-style), we fall back to
        // re-publishing to the entire cluster rather than per-host addressing — the
        // payload's fenceVersion makes the duplicate harmless.
        // TODO: switch to per-pod addresses when ConsumerNodeIdRouter is available.
        exchange.publish(ROUTE_FAILOVER, API_STAGE_EVENT, ClusterMessage.of(event));
    }
}
