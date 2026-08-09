package com.flipkart.varadhi.core.cluster.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;

/**
 * Pod-side contract for topic-transition stage broadcasts.
 *
 * <p>Subscribe via {@link TransitionBus#subscribe}; do not register string publish routes
 * directly on {@code MessageRouter}.
 */
@FunctionalInterface
public interface TransitionEventListener {

    void onTransition(TransitionEvent event);
}
