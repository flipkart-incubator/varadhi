package com.flipkart.varadhi.web.metrics;

/**
 * Interface for emitting HTTP API-related metrics in the Varadhi system.
 * Implementations should handle the recording of various API metrics including
 * latencies, request counts, and error states.
 *
 * <p>This interface extends {@link AutoCloseable} to ensure proper cleanup of resources
 * when the emitter is no longer needed.</p>
 *
 * @see HttpApiMetricsEmitterImpl
 * @see HttpApiMetricsEmitterNoOpImpl
 */
public interface HttpApiMetricsEmitter extends AutoCloseable {

    /**
     * Records metrics for a successful API request.
     *
     * @param statusCode the HTTP status code of the response
     * @throws IllegalArgumentException if status code is invalid
     */
    void recordSuccess(int statusCode);

    /**
     * Records metrics for a failed API request.
     *
     * @param statusCode the HTTP status code of the response
     * @param errorType  the type of error that occurred
     * @throws IllegalArgumentException if status code is invalid or error type is null
     */
    void recordError(int statusCode, String errorType);

    /**
     * Closes this metrics emitter and releases any system resources associated with it.
     * Implementations should ensure that any buffered metrics are properly flushed.
     */
    @Override
    void close();
}
