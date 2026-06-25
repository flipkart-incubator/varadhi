package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;

/**
 * Sink for pod-to-controller topic-transition stage acknowledgments. Implemented by
 * {@link ControllerTransitionAckClient} in production; abstracted as an interface so the
 * stage handler can be unit-tested without a live cluster bus.
 */
public interface TransitionAckClient {

    /**
     * Sends a single stage {@link TransitionAck} from this pod back to the controller.
     * Implementations are best-effort: delivery failures are handled by the controller's
     * stage-barrier timeout (which re-pushes the stage event), so callers need not retry.
     *
     * @param ack the per-stage acknowledgment to deliver
     */
    void ack(TransitionAck ack);
}
