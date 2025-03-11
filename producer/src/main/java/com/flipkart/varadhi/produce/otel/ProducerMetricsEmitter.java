package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.produce.config.ProducerErrorType;

/**
 * Interface for emitting producer-related metrics in the Varadhi messaging system.
 * Implementations should handle the recording of various producer metrics including
 * latencies, message sizes, and error states.
 *
 * <p>This interface extends {@link AutoCloseable} to ensure proper cleanup of resources
 * when the emitter is no longer needed.</p>
 *
 * <p>Usage example:
 * <pre>{@code
 * try (ProducerMetricsEmitter emitter = metricsHandler.getEmitter(attributes)) {
 *     emitter.emit(true, 100L, 50L, 1024, false, null);
 * }
 * }</pre>
 * </p>
 *
 * @see ProducerMetricsEmitterImpl
 * @see ProducerMetricsEmitterNoOpImpl
 */
public interface ProducerMetricsEmitter extends AutoCloseable {

    /**
     * Emits metrics for a produce operation.
     *
     * @param succeeded       whether the produce operation was successful
     * @param producerLatency the total time taken for the produce operation in milliseconds
     * @param storageLatency  the time taken for storage operations in milliseconds
     * @param messageSize     the size of the message in bytes
     * @param filtered        whether the message was filtered
     * @param errorType       the type of error if the operation failed, null if successful
     * @throws IllegalArgumentException if latencies are negative or message size is invalid
     */
    void emit(boolean succeeded, long producerLatency, long storageLatency, int messageSize,
              boolean filtered, ProducerErrorType errorType);

    /**
     * Closes this metrics emitter and releases any system resources associated with it.
     * Implementations should ensure that any buffered metrics are properly flushed.
     */
    @Override
    void close();
}
