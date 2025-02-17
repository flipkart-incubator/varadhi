package com.flipkart.varadhi.metrics;

import com.flipkart.varadhi.entities.auth.ResourceType;
import io.micrometer.core.instrument.Timer;

/**
 * Emitter interface for event-related metrics in Varadhi.
 * <p>
 * This interface provides methods to track various event metrics including:
 * <ul>
 *     <li>Event creation latency</li>
 *     <li>Success/failure counts</li>
 *     <li>Error type distribution</li>
 * </ul>
 * <p>
 * Usage example:
 * <pre>{@code
 * try (var metrics = EventMetricsFactory.create(config)) {
 *     Timer.Sample sample = metrics.startTimer();
 *     try {
 *         // Perform event operation
 *         metrics.recordCreationSuccess(sample, resourceType);
 *     } catch (Exception e) {
 *         metrics.recordCreationError(sample, resourceType, e.getClass().getSimpleName());
 *         throw e;
 *     }
 * }
 * }</pre>
 *
 * @see EventMetrics
 * @see Timer.Sample
 */
public sealed interface EventMetricsEmitter extends AutoCloseable permits EventMetrics, NoOpEventMetricsEmitter {

    /**
     * Starts timing an event operation.
     * <p>
     * The returned sample should be used with either {@link #recordCreationSuccess}
     * or {@link #recordCreationError} to complete the timing.
     *
     * @return A new Timer.Sample instance
     * @throws IllegalStateException if the metrics registry is not available
     */
    Timer.Sample startTimer();

    /**
     * Records a successful event creation operation.
     * <p>
     * This method:
     * <ul>
     *     <li>Stops the timing sample</li>
     *     <li>Records the latency</li>
     *     <li>Increments the success counter</li>
     * </ul>
     *
     * @param sample Timer sample from {@link #startTimer()}
     * @param type   Resource type for the event
     * @throws IllegalArgumentException if the resource type is not supported
     * @throws NullPointerException     if sample or type is null
     */
    void recordCreationSuccess(Timer.Sample sample, ResourceType type);

    /**
     * Records a failed event creation operation.
     * <p>
     * This method:
     * <ul>
     *     <li>Stops the timing sample</li>
     *     <li>Records the latency</li>
     *     <li>Increments the error counter for the specific error type</li>
     * </ul>
     *
     * @param sample    Timer sample from {@link #startTimer()}
     * @param type      Resource type for the event
     * @param errorType Classification of the error encountered
     * @throws IllegalArgumentException if the resource type is not supported
     * @throws NullPointerException     if any parameter is null
     */
    void recordCreationError(Timer.Sample sample, ResourceType type, String errorType);

    /**
     * Closes and cleans up all metrics registered by this emitter.
     * <p>
     * This method should be called when the emitter is no longer needed to prevent
     * memory leaks in the metrics registry.
     */
    @Override
    void close();
}
