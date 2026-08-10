package com.flipkart.varadhi.core.cluster.failover;

import lombok.experimental.UtilityClass;

/**
 * Cluster-bus address parts for topic-transition coordination (topic failover,
 * storage-topic migration).
 *
 * <p>Addresses are structured like an HTTP path — a {@code route} that groups a family of
 * apis, and an {@code api} that names the specific operation — so transition addresses sit
 * predictably alongside other routes ({@code <route>.<api>.<method>}):
 *
 * <ul>
 *   <li><b>Forward leg (controller → all pods):</b> {@code TransitionPublisher#broadcastEvent}
 *       ({@code TransitionService}) publishes a {@code TransitionEvent} to
 *       {@code ROUTE_TOPIC_TRANSITION + "." + EVENT_PUBLISH_API + ".publish"}; every pod
 *       registers via {@code TransitionEventSubscriber.subscribe}. Not part of
 *       {@code ControllerRemoteClient}.</li>
 *   <li><b>Back leg (pod → controller):</b> a pod sends a {@code TransitionAck} via
 *       {@code TransitionAckApi#ack} ({@code ControllerRemoteClient}) to
 *       {@code <controllerRoute>." + STAGE_ACK_API + ".send"} (the controller route is
 *       {@code ControllerApi.ROUTE_CONTROLLER}).</li>
 * </ul>
 */
@UtilityClass
public final class TransitionBusAddress {

    /** Route grouping all topic-transition bus apis. */
    public final String ROUTE_TOPIC_TRANSITION = "topic.transition";

    /** Api (under {@link #ROUTE_TOPIC_TRANSITION}) on which the per-stage event is broadcast to all pods. */
    public final String EVENT_PUBLISH_API = "event.publish";

    /** Api (under the controller route) for the pod-to-controller per-stage acknowledgement. */
    public final String TRANSITION_EVENT_ACK_API = "topic.transition.event.ack";
}
