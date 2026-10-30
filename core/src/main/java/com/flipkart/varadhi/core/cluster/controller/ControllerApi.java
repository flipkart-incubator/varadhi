package com.flipkart.varadhi.core.cluster.controller;

/**
 * Cluster-facing controller API: subscription lifecycle + topic-transition acks + failover RPCs.
 * Consumer shard callbacks live on {@link SubscriptionApi#update}.
 *
 * <p>Broadcast of transition stage events is controller-local ({@link TransitionPublisher}),
 * not part of this remote-callable surface.
 */
public interface ControllerApi extends SubscriptionApi, TransitionAckApi, FailoverApi {
    String ROUTE_CONTROLLER = "controller";
}
