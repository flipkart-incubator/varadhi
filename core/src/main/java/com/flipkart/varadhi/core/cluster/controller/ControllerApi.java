package com.flipkart.varadhi.core.cluster.controller;

/**
 * Cluster-facing controller API for subscription lifecycle.
 * Consumer shard callbacks live on {@link ConsumerCallbackApi}.
 */
public interface ControllerApi extends SubscriptionApi {
    String ROUTE_CONTROLLER = "controller";
}
