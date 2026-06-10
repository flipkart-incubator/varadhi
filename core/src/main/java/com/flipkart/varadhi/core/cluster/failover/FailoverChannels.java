package com.flipkart.varadhi.core.cluster.failover;

/**
 * Cluster-bus addressing constants for topic-failover coordination.
 *
 * <ul>
 *   <li><b>Forward leg (controller → all pods):</b> the controller publishes
 *       {@code FailoverStageEvent} to {@code FAILOVER_ROUTE.STAGE_EVENT_API.publish};
 *       every pod registers a {@code publishHandler} there.</li>
 *   <li><b>Back leg (pod → controller):</b> a pod sends {@code FailoverStatusUpdate}
 *       to {@code controller.ACK_API.send} (the controller route is
 *       {@code ControllerApi.ROUTE_CONTROLLER}).</li>
 * </ul>
 */
public final class FailoverChannels {

    /** Broadcast route on which the failover stage-event is published to all pods. */
    public static final String FAILOVER_ROUTE = "failover";

    /** Api name (under {@link #FAILOVER_ROUTE}) for the broadcast stage event. */
    public static final String STAGE_EVENT_API = "stageEvent";

    /** Api name (under the controller route) for the pod-to-controller ack. */
    public static final String ACK_API = "failoverAck";

    private FailoverChannels() {
    }
}
