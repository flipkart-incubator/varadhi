package com.flipkart.varadhi.produce.failover;

import com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate;

/**
 * Sink for pod-to-controller failover stage acknowledgments. Implemented by
 * {@link ControllerFailoverClient} in production; abstracted as an interface so the
 * stage handler can be unit-tested without a live cluster bus.
 */
public interface FailoverAcker {
    void ack(FailoverStatusUpdate update);
}
