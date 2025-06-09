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
     * @param msgSizeBytes The total size of the received message in bytes. Includes headers and body.
     */
    void received(int payloadSizeBytes, int msgSizeBytes);

    /**
     * Records the result of the message production operation.
     *
     * @param result The result of the produce operation, containing message ID and status.
     * @param t The throwable if the produce operation failed, null if it succeeded.
     */
    void accepted(ProduceResult result, Throwable t);

    void close();

    ProducerMetrics NOOP = new NoOpImpl();

    class NoOpImpl implements ProducerMetrics {

        @Override
        public void received(int payloadSizeBytes, int msgSizeBytes) {
        }

        @Override
        public void accepted(ProduceResult result, Throwable t) {
        }

        @Override
        public void close() {
        }
    }
}
