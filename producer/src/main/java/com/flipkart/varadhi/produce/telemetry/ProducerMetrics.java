package com.flipkart.varadhi.produce.telemetry;

import com.flipkart.varadhi.produce.ProducerErrorType;

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

    void filtered();

    /**
     * Records a successful message production with the given latency.
     *
     * @param latencyMs The messaging stack latency in milliseconds for the message production.
     */
    void success(long latencyMs);

    /**
     *
     * @param errorType
     * @param latencyMs Can be -1 if the latency is not applicable or unknown, which is applicable when the exception
     *                  didn't arise from the messaging stack.
     */
    void failure(ProducerErrorType errorType, long latencyMs);

    void close();

    ProducerMetrics NOOP = new NoOpImpl();

    class NoOpImpl implements ProducerMetrics {

        @Override
        public void received(int payloadSizeBytes, int msgSizeBytes) {
        }

        @Override
        public void filtered() {
        }

        @Override
        public void success(long latencyMs) {
        }

        @Override
        public void failure(ProducerErrorType errorType, long latencyMs) {
        }

        @Override
        public void close() {
        }
    }
}
