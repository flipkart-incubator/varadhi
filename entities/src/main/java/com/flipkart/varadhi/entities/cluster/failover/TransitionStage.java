package com.flipkart.varadhi.entities.cluster.failover;

/**
 * Cluster-wide stage of a topic transition (e.g. topic failover, storage-topic migration).
 * The controller drives a {@code TransitionMaster} through these stages and waits on a
 * per-pod ack barrier at <b>every</b> stage it broadcasts — each pod acknowledges every
 * stage so the controller can confirm fleet-wide progress and abort/remediate on a missing
 * or failed ack.
 *
 * <p>This enum is part of the pod-facing <b>wire contract</b> (it travels inside
 * {@link TransitionEvent} and {@link TransitionAck}). It carries no controller-only state.
 *
 * <p>Typical topic-failover order on the controller executor:
 * {@link #PREPARE} → {@link #FENCE} → {@link #MIGRATE} → {@link #COMPLETED}:
 * <ul>
 *   <li>{@link #PREPARE} — readiness: pod waits for topic version N and may warm the target.</li>
 *   <li>{@link #FENCE} — fence ingress ({@code state=Fenced}); stop new produce before lag wait.</li>
 *   <li>{@link #MIGRATE} — lag poll then commit route ({@code Producing} + {@code failOverRegion}).</li>
 *   <li>{@link #PENDING}, {@link #COMPLETED}, {@link #ABORTED} — lifecycle markers.</li>
 * </ul>
 */
public enum TransitionStage {
    PENDING, PREPARE, FENCE, MIGRATE, COMPLETED, ABORTED;

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED;
    }

    /**
     * Abort is honored through {@link #FENCE} and while {@link #MIGRATE} has not yet committed
     * the route (executor enforces pre-commit). After the MIGRATE topic write, abort is refused.
     */
    public boolean isAbortable() {
        return this == PENDING || this == PREPARE || this == FENCE || this == MIGRATE;
    }

    /** Stages where the pod must observe {@code topicVersionToAwait} before acking. */
    public boolean needsTopicVersionSync() {
        return this == PREPARE || this == FENCE || this == MIGRATE;
    }
}
