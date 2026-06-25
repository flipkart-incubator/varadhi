package com.flipkart.varadhi.produce.failover;

/**
 * Pod-side timing knobs for topic-transition stage handling (shared by all
 * {@link com.flipkart.varadhi.entities.cluster.failover.TransitionType}s, not failover-specific).
 *
 * @param podVersionWaitMs  max time a pod waits for its TopicCache to reach the target
 *                          version (PREPARE=N, SWITCH=N+1) before acking failure
 * @param podPollIntervalMs sleep between TopicCache version checks (L1 propagation is
 *                          typically sub-100ms)
 */
public record PodTransitionConfig(long podVersionWaitMs, long podPollIntervalMs) {

    public static PodTransitionConfig defaultConfig() {
        return new PodTransitionConfig(5000L, 25L);
    }
}
