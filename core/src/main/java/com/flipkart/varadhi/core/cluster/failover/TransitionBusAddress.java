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
 *   <li><b>Forward leg (controller → all pods):</b> {@code TransitionService#sendEvent}
 *       publishes a {@code TransitionEvent} to
 *       {@code ROUTE_TOPIC_TRANSITION + "." + EVENT_PUBLISH_API + ".publish"}; every pod
 *       registers a {@code registerPublishReceiveHandler} there. Remote clients do not
 *       publish — {@code ControllerRemoteClient#sendEvent} throws.</li>
 *   <li><b>Back leg (pod → controller):</b> a pod sends a {@code TransitionAck} via
 *       {@code ControllerRemoteClient#ack} to
 *       {@code <controllerRoute>." + TRANSITION_EVENT_ACK_API + ".send"} (the controller route is
 *       {@code ControllerApi.ROUTE_CONTROLLER}).</li>
 * </ul>
 */
@UtilityClass
public final class TransitionBusAddress {

    /** Route grouping all topic-transition bus apis. */
    public final String ROUTE_TOPIC_TRANSITION = "topic.transition";

    /** Api (under {@link #ROUTE_TOPIC_TRANSITION}) on which the per-stage event is broadcast to all pods. */
    public final String EVENT_PUBLISH_API = "event.publish";

    /** Alias for {@link #EVENT_PUBLISH_API}; used on the controller broadcast path. */
    public final String STAGE_BROADCAST_API = EVENT_PUBLISH_API;

    /** Api (under the controller route) for the pod-to-controller per-stage acknowledgement. */
    public final String STAGE_ACK_API = "stage.ack";

    /** Controller request APIs (web → controller) for the failover lifecycle. */
    public final String CREATE_FAILOVER_API = "createFailover";
    public final String GET_FAILOVER_API = "getFailover";
    public final String ABORT_FAILOVER_API = "abortFailover";
    public final String LIST_FAILOVERS_API = "listFailovers";
}
