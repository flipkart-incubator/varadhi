package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;

import java.util.concurrent.CompletableFuture;

/**
 * Pod → controller back-leg for topic-transition stage acknowledgements.
 *
 * <p>Implemented remotely by {@link ControllerRemoteClient} over the controller route, and
 * in-process by {@code TransitionService} on the controller.
 */
public interface TransitionAckApi {

    /**
     * Delivers a topic-transition stage acknowledgment from a pod to the controller.
     */
    CompletableFuture<Void> ack(TransitionAck ack);
}
