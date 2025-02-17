package com.flipkart.varadhi.metrics;

import com.flipkart.varadhi.entities.auth.ResourceType;
import io.micrometer.core.instrument.Timer;

/**
 * No-op implementation of EventMetricsEmitter.
 * <p>
 * Used when metrics are disabled to avoid conditional logic in business code.
 */
public final class NoOpEventMetricsEmitter implements EventMetricsEmitter {
    public static final NoOpEventMetricsEmitter INSTANCE = new NoOpEventMetricsEmitter();

    private NoOpEventMetricsEmitter() {
        // Singleton
    }

    @Override
    public Timer.Sample startTimer() {
        return null;
    }

    @Override
    public void recordCreationSuccess(Timer.Sample sample, ResourceType type) {
        // No-op implementation - metrics are disabled
    }

    @Override
    public void recordCreationError(Timer.Sample sample, ResourceType type, String errorType) {
        // No-op implementation - metrics are disabled
    }

    @Override
    public void close() {
        // No-op implementation - no resources to clean up
    }
}
