package com.flipkart.varadhi.produce.telemetry;

import com.flipkart.varadhi.produce.ProducerErrorType;

/**
 * Interface for recording producer-related metrics in the Varadhi messaging system.
 * Implementations should handle the recording of various producer metrics including
 * latencies, message sizes, and error states.
 *
 * @see ProducerMetricsRecorderImpl
 * @see NoOpImpl
 */
public interface ProducerMetricsRecorder {

    void received(int msgSizeBytes);

    void success(long latencyMs);

    void failure(ProducerErrorType errorType, long latencyMs);

    void close();

    class NoOpImpl implements ProducerMetricsRecorder {

        @Override
        public void received(int msgSizeBytes) {
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
