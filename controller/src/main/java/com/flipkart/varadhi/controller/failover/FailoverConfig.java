package com.flipkart.varadhi.controller.failover;

import lombok.Builder;
import lombok.Getter;

/**
 * Tunables for the topic-failover executor. All values are in milliseconds unless noted.
 *
 * <p>Defaults are conservative for production; tests typically override
 * {@code timeoutMs}/{@code podSwitchWaitMs} to single-digit seconds.
 */
@Getter
@Builder
public class FailoverConfig {

    /** Per-stage ack barrier timeout (PREPARE, SWITCH). */
    @Builder.Default
    private long stageAckTimeoutMs = 60_000L;

    /** After this many ms the awaiter targeted-resends to hosts that have not yet acked. */
    @Builder.Default
    private long stageAckResendAfterMs = 15_000L;

    /** Pod-side max wait inside the ACK_TRIGGER handler for the L1 cache to catch up. */
    @Builder.Default
    private long podSwitchWaitMs = 30_000L;

    /** Pod-side poll interval while waiting for cache catch-up. */
    @Builder.Default
    private long podPollIntervalMs = 200L;

    /** Controller-side poll interval while waiting for DRAIN lag to clear. */
    @Builder.Default
    private long drainPollIntervalMs = 1_000L;

    /** Hard cap on how long the controller will wait for DRAIN before giving up. */
    @Builder.Default
    private long drainTimeoutMs = 5 * 60_000L;

    /** Threads in the {@link StageAwaiterRegistry} timeout/resend scheduler. */
    @Builder.Default
    private int stageAwaiterSchedulerThreads = 2;

    public static FailoverConfig defaults() {
        return FailoverConfig.builder().build();
    }
}
