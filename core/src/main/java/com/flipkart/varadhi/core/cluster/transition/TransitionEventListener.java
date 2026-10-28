package com.flipkart.varadhi.core.cluster.transition;

import com.flipkart.varadhi.entities.cluster.transition.TransitionEvent;

/**
 * Pod-side contract for topic-transition stage broadcasts.
 *
 * <p>Subscribe via {@link TransitionEventSubscriber#subscribe}; do not register string publish
 * routes directly on {@code MessageRouter}.
 */
@FunctionalInterface
public interface TransitionEventListener {

    void onTransition(TransitionEvent event);
}
