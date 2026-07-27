package com.flipkart.varadhi.core.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pod-side tuning for a topic-transition stage wait (PREPARE/SWITCH stage barriers): how long and
 * how often a pod polls its TopicCache for the coordinated topic version, plus a test-only ack
 * delay knob. Split out of {@link ProducerOptions} so transition-only settings are grouped under
 * their own {@code producerOptions.transition} config namespace.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProduceTransitionOptions {

    /**
     * Approximate upper bound (ms) a pod waits for its TopicCache to reach the coordinated topic
     * version during a topic-transition stage (PREPARE/SWITCH) before acking failure. Mapped to a
     * fixed number of poll attempts, so actual wait is roughly
     * {@code (ceil(waitMs / pollIntervalMs) - 1) * pollIntervalMs}.
     */
    @Builder.Default
    private long versionWaitMs = 1000;

    /** Fixed poll interval (ms) between TopicCache version checks during a topic-transition stage. */
    @Builder.Default
    private long pollIntervalMs = 25;

    /**
     * Test-only delay (ms) before a pod reports a SWITCH-stage {@code TransitionAck} to the
     * controller. Extends the produce-blocked window while the stage barrier is open. {@code 0}
     * disables the delay.
     */
    @Builder.Default
    private long ackReportDelayMs = 0;

    public static ProduceTransitionOptions defaultOptions() {
        return ProduceTransitionOptions.builder().build();
    }
}
