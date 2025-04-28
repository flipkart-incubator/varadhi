package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.produce.config.ProducerErrorType;

/**
 * A no-operation implementation of {@link ProducerMetricsEmitter} that discards all metrics.
 * This implementation is used when metrics collection is disabled to avoid the overhead
 * of metric recording while maintaining the same interface contract.
 *
 * <p>This class is thread-safe and immutable.</p>
 *
 * <p>Usage example:
 * <pre>{@code
 * ProducerMetricsEmitter emitter = new ProducerMetricsEmitterNoOpImpl();
 * emitter.emit(true, 100L, 50L, 1024, false, null); // Metrics are discarded
 * }</pre>
 * </p>
 *
 * @see ProducerMetricsEmitter
 */
public final class ProducerMetricsEmitterNoOpImpl implements ProducerMetricsEmitter {

    /**
     * Discards the provided metrics without recording them.
     *
     * @param succeeded       whether the produce operation was successful
     * @param producerLatency the total time taken for the produce operation in milliseconds (ignored)
     * @param storageLatency  the time taken for storage operations in milliseconds (ignored)
     * @param messageSize     the size of the message in bytes (ignored)
     * @param filtered        whether the message was filtered (ignored)
     * @param errorType       the type of error if the operation failed (ignored)
     */
    @Override
    public void emit(
        boolean succeeded,
        long producerLatency,
        long storageLatency,
        int messageSize,
        boolean filtered,
        ProducerErrorType errorType
    ) {
        // Intentionally empty - no metrics are recorded
    }

    /**
     * No-op implementation of close - no resources to clean up.
     */
    @Override
    public void close() {
        // Intentionally empty - no resources to clean up
    }
}
