package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.failover.TransitionEvent;

import java.util.concurrent.CompletableFuture;

/**
 * Controller-local forward leg: broadcast a stage event to all produce pods.
 *
 * <p>Not part of {@link ControllerApi} — pods never call this; only the controller orchestrator
 * (via {@code TransitionService}) publishes.
 */
public interface TransitionPublisher {

    /**
     * Broadcasts a stage event to all produce pods on the topic-transition publish route.
     */
    CompletableFuture<Void> broadcastEvent(TransitionEvent event);
}
