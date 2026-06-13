package com.flipkart.varadhi.produce.telemetry;

import com.flipkart.varadhi.produce.ProduceResult;

/**
 * Interface for recording producer-related metrics in the Varadhi messaging system.
 * Implementations should handle the recording of various producer metrics including
 * latencies, message sizes, and error states.
 *
 * @see ProducerMetricsImpl
 * @see NoOpImpl
 */
public interface ProducerMetrics {

    /**
     * Records the receipt of a message with the specified size in bytes.
     *
     * @param payloadSizeBytes body size in bytes
     * @param msgSizeBytes total size including headers and body
     */
    void received(int payloadSizeBytes, int msgSizeBytes);

    /**
     * Records the result of the message production operation.
     *
     * @param result the produce outcome
     * @param t failure from the async path, or {@code null} on completion
     * @param messageBytes total message size for rejection byte accounting
     */
    void accepted(ProduceResult result, Throwable t, long messageBytes);

    /**
     * Records a rate-limiter rejection in shadow mode — produce was still allowed.
     * Uses {@code producer.rejected.*} with {@code shadow=true}.
     */
    void shadowRejected(long messageBytes);

    void close();

    ProducerMetrics NOOP = new NoOpImpl();

    class NoOpImpl implements ProducerMetrics {

        @Override
        public void received(int payloadSizeBytes, int msgSizeBytes) {
        }

        @Override
        public void accepted(ProduceResult result, Throwable t, long messageBytes) {
        }

        @Override
        public void shadowRejected(long messageBytes) {
        }

        @Override
        public void close() {
        }
    }
}
