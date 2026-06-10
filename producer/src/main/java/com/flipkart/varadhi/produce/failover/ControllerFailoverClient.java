package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.core.cluster.failover.FailoverChannels;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate;
import lombok.extern.slf4j.Slf4j;

/**
 * Pod-side client that sends failover stage acknowledgments back to the controller.
 *
 * <p>Uses {@link MessageExchange#send} (delivery-tracked, no response body) to the
 * controller route. Acks are best-effort from the pod's perspective: if delivery
 * fails, the controller's stage barrier will time out and re-push the stage event to
 * the missing host, so the pod simply logs and moves on.
 */
@Slf4j
public final class ControllerFailoverClient implements FailoverAcker {

    private final MessageExchange exchange;

    public ControllerFailoverClient(MessageExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public void ack(FailoverStatusUpdate update) {
        ClusterMessage message = ClusterMessage.of(update);
        exchange.send(ControllerApi.ROUTE_CONTROLLER, FailoverChannels.ACK_API, message).exceptionally(t -> {
            log.warn(
                "Failed to deliver failover ack op={} stage={} host={}: {}",
                update.opId(),
                update.stage(),
                update.hostname(),
                t.getMessage()
            );
            return null;
        });
    }
}
