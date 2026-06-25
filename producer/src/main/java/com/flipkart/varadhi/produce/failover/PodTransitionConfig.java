package com.flipkart.varadhi.produce.failover;

/**
 * Pod-side timing knobs for topic-transition stage handling (shared by all
 * {@link com.flipkart.varadhi.entities.cluster.failover.TransitionType}s, not failover-specific).
 *
 * @param podVersionWaitMs  approximate upper bound (ms) a pod waits for its TopicCache to reach
 *                          the target version (PREPARE=N, SWITCH=N+1) before acking failure; the
 *                          handler maps this to {@code ceil(waitMs / pollIntervalMs)} Failsafe
 *                          attempts, so actual wait is roughly {@code (attempts - 1) * pollIntervalMs}
 * @param podPollIntervalMs fixed delay (ms) between TopicCache version checks (L1 propagation is
 *                          typically sub-100ms)
 */
public record PodTransitionConfig(long podVersionWaitMs, long podPollIntervalMs) {

    public PodTransitionConfig {
        if (podVersionWaitMs <= 0) {
            throw new IllegalArgumentException("podVersionWaitMs must be positive");
        }
        if (podPollIntervalMs <= 0) {
            throw new IllegalArgumentException("podPollIntervalMs must be positive");
        }
    }

    public static PodTransitionConfig defaultConfig() {
        return new PodTransitionConfig(5000L, 25L);
    }
}
