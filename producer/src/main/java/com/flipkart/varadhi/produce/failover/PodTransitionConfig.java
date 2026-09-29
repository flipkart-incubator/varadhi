package com.flipkart.varadhi.produce.failover;

/**
 * Pod-side timing for topic-transition stage handling (shared by all
 * {@link com.flipkart.varadhi.entities.cluster.failover.TransitionType}s, not failover-specific).
 *
 * @param versionWaitMs approximate upper bound (ms) a pod waits for its TopicCache to reach the
 *                      target version before acking failure
 * @param pollIntervalMs delay (ms) between TopicCache version checks; {@code 0} means no delay
 *                       (retry immediately). Attempt count uses {@code max(1, pollIntervalMs)} as
 *                       the divisor so {@code versionWaitMs} still bounds the wait when delay is 0.
 */
public record PodTransitionConfig(long versionWaitMs, long pollIntervalMs) {

    public static PodTransitionConfig defaultConfig() {
        return new PodTransitionConfig(5000L, 25L);
    }

    /** Failsafe max-attempts: {@code ceil(versionWaitMs / max(1, pollIntervalMs))}. */
    public int versionWaitMaxAttempts() {
        long step = Math.max(1L, pollIntervalMs);
        return Math.max(1, (int)Math.ceil((double)versionWaitMs / step));
    }
}
