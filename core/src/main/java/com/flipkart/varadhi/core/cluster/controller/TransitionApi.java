package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;

import java.util.concurrent.CompletableFuture;

/**
 * Topic-transition coordination APIs.
 *
 * <p>{@link #sendEvent} is controller-local (broadcast to pods). {@link #ack} is the pod →
 * controller back-leg over the controller route.
 */
public interface TransitionApi {

    /**
     * Broadcasts a stage event to all produce pods.
     * Remote clients may leave this unsupported; call the controller-local service instead.
     */
    CompletableFuture<Void> sendEvent(TransitionEvent event);

    /**
     * Delivers a topic-transition stage acknowledgment from a pod to the controller.
     */
    CompletableFuture<Void> ack(TransitionAck ack);
}
