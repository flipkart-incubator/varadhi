package com.flipkart.varadhi.entities.cluster.failover;

/**
 * The kind of topic transition a {@link TransitionEvent} drives. The pod-side handler
 * and the controller orchestration share the same stage machine
 * ({@link TransitionStage}), ack barrier and version-convergence mechanics; only the
 * per-stage work and the meaning of {@link TransitionEvent#target()} differ per type.
 *
 * <ul>
 *   <li>{@link #TOPIC_FAILOVER} — switch produce authority for a topic from one region
 *       to another. {@code target} is a {@link TransitionEvent.Target.Region}.</li>
 *   <li>{@link #STORAGE_MIGRATION} — move a topic's active produce target from one
 *       backing {@code StorageTopic} (segment) to another within the same region (e.g.
 *       a Pulsar cluster/topic move). {@code target} is a
 *       {@link TransitionEvent.Target.StorageTopic}.</li>
 * </ul>
 */
public enum TransitionType {
    TOPIC_FAILOVER, STORAGE_MIGRATION
}
