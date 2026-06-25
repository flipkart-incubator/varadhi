package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Pod-side client that sends topic-transition stage acknowledgments back to the controller.
 *
 * <p>Uses {@link MessageExchange#send} (delivery-tracked, no response body) to the controller
 * route. Acks are best-effort from the pod's perspective: if delivery fails, the controller's
 * stage barrier will time out and re-push the stage event to the missing host, so the pod
 * simply logs and moves on.
 */
@Slf4j
@AllArgsConstructor
public final class ControllerTransitionAckClient implements TransitionAckClient {

    private final MessageExchange exchange;

    @Override
    public void ack(TransitionAck ack) {
        ClusterMessage message = ClusterMessage.of(ack);
        exchange.send(ControllerApi.ROUTE_CONTROLLER, TransitionBusAddress.STAGE_ACK_API, message).exceptionally(t -> {
            log.warn(
                "Failed to deliver transition ack op={} stage={} host={}",
                ack.opId(),
                ack.stage(),
                ack.hostname(),
                t
            );
            return null;
        });
    }
}
