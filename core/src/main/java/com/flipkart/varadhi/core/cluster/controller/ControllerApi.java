package com.flipkart.varadhi.core.cluster.controller;

/**
 * Cluster-facing controller API: subscription lifecycle + topic-transition coordination.
 * Consumer shard callbacks live on {@link ConsumerCallbackApi}.
 */
public interface ControllerApi extends SubscriptionApi, TransitionApi {
    String ROUTE_CONTROLLER = "controller";
}
