package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.EventMarker;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.metrics.EventMetricsEmitter;
import com.flipkart.varadhi.metrics.EventMetricsFactory;
import com.flipkart.varadhi.spi.db.EventStore;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Set;

/**
 * Service responsible for creating event markers that track resource changes in the system.
 * <p>
 * This service provides thread-safe operations for creating and tracking event markers using
 * a distributed event store. It supports metrics collection for monitoring and analysis.
 * <p>
 * Usage example:
 * <pre>{@code
 * try (var service = new EventService(eventStore, registry, true)) {
 *     service.createEventMarker("myResource", ResourceType.TOPIC);
 * }
 * }</pre>
 *
 * @see EventMarker
 * @see EventStore
 * @see EventMetricsEmitter
 */
@Slf4j
public class EventService {
    private final EventStore eventStore;
    private final EventMetricsEmitter metrics;

    private static final String EVENT_NAME_FORMAT = "event_%d";

    /**
     * Creates a new EventService instance with optional metrics collection.
     * <p>
     * The service will track metrics for all resource types defined in {@link ResourceType}.
     * When metrics are enabled, both successful and failed operations are tracked with
     * appropriate tags and dimensions.
     *
     * @param eventStore     The event store implementation to use
     * @param meterRegistry  The metrics registry for collecting metrics
     * @param metricsEnabled Whether to enable metrics collection
     * @throws NullPointerException if eventStore is null
     */
    public EventService(EventStore eventStore, MeterRegistry meterRegistry, boolean metricsEnabled) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore cannot be null");
        this.metrics = EventMetricsFactory.create(
            new EventMetricsFactory.MetricsConfig(metricsEnabled, meterRegistry, Set.of(ResourceType.values()))
        );
    }

    /**
     * Creates an event marker for a resource change.
     * <p>
     * This method is thread-safe and uses a distributed sequence number to ensure
     * uniqueness across the cluster. It tracks the following metrics when enabled:
     * <ul>
     *     <li>Creation latency</li>
     *     <li>Success/failure counts</li>
     *     <li>Error types when failures occur</li>
     * </ul>
     *
     * @param resourceName The name of the resource being tracked
     * @param resourceType The type of resource being tracked
     * @throws MetaStoreException       if event creation fails in the underlying store
     * @throws IllegalArgumentException if resourceName is null or blank, or if resourceType is null
     */
    public synchronized void createEventMarker(String resourceName, ResourceType resourceType) {
        validateInputs(resourceName, resourceType);

        Timer.Sample sample = metrics.startTimer();
        try {
            long sequenceNumber = eventStore.getNextSequenceNumber();
            String eventName = String.format(EVENT_NAME_FORMAT, sequenceNumber);

            EventMarker eventMarker = EventMarker.of(eventName, resourceName, resourceType);

            eventStore.createEvent(eventMarker);
            metrics.recordCreationSuccess(sample, resourceType);

            log.debug("Created event marker {} for resource {} of type {}", eventName, resourceName, resourceType);
        } catch (Exception e) {
            metrics.recordCreationError(sample, resourceType, e.getClass().getSimpleName());
            log.error(
                "Failed to create event marker for resource {} of type {}: {}",
                resourceName,
                resourceType,
                e.getMessage()
            );
            throw new MetaStoreException("Failed to create event marker", e);
        }
    }

    /**
     * Validates the input parameters for event marker creation.
     *
     * @param resourceName The resource name to validate
     * @param resourceType The resource type to validate
     * @throws IllegalArgumentException if any parameter is invalid
     */
    private void validateInputs(String resourceName, ResourceType resourceType) {
        if (resourceName == null || resourceName.isBlank()) {
            throw new IllegalArgumentException("Resource name cannot be null or blank");
        }
        if (resourceType == null) {
            throw new IllegalArgumentException("Resource type cannot be null");
        }
    }
}
