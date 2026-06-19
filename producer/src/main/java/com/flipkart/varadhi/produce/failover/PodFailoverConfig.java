package com.flipkart.varadhi.produce.failover;

/**
 * Pod-side timing knobs for failover stage handling.
 *
 * @param podSwitchWaitMs   max time a pod waits for its TopicCache to reach the
 *                          target version during SWITCH before acking failure
 * @param podPollIntervalMs sleep between TopicCache version checks (L1 propagation
 *                          is typically sub-100ms)
 */
public record PodFailoverConfig(long podSwitchWaitMs, long podPollIntervalMs) {

    public static PodFailoverConfig defaultConfig() {
        return new PodFailoverConfig(5000L, 25L);
    }
}
