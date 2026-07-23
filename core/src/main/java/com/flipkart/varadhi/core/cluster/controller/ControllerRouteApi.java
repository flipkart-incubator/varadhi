package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;

import java.util.concurrent.CompletableFuture;

/**
 * Pod → controller APIs delivered over the controller route via {@code send}
 * (delivery-tracked, no response body). Implemented by {@link ControllerRouteClient}
 * on pods and by the controller-side handler/manager.
 */
public interface ControllerRouteApi {
    String ROUTE_CONTROLLER = "controller";

    CompletableFuture<Void> update(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg);

    /**
     * Delivers a topic-transition stage acknowledgment from a pod back to the controller.
     */
    CompletableFuture<Void> ackTopicTransition(TransitionAck ack);
}
