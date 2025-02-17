package com.flipkart.varadhi.metrics;

import com.flipkart.varadhi.entities.auth.ResourceType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of EventMetricsEmitter that tracks event-related metrics.
 * Thread-safe and supports concurrent access from multiple threads.
 *
 * @see EventMetricsEmitter
 */
@Slf4j
public final class EventMetrics implements EventMetricsEmitter {
    private final MeterRegistry registry;
    private final Set<ResourceType> supportedTypes;
    private final ConcurrentHashMap<ResourceType, MetricSet> metricsByType;

    /**
     * Groups related metrics for a specific resource type.
     * Provides atomic access to all metrics for a given resource type.
     *
     * @param latency Timer for tracking operation duration
     * @param success Counter for successful operations
     * @param errors  Map of error type to error counters
     */
    private record MetricSet(Timer latency, Counter success, ConcurrentHashMap<String, Counter> errors) {
    }

    /**
     * Creates a new EventMetrics instance.
     *
     * @param registry       The metrics registry to use
     * @param supportedTypes Set of resource types to track metrics for
     * @throws NullPointerException if registry or supportedTypes is null
     */
    public EventMetrics(MeterRegistry registry, Set<ResourceType> supportedTypes) {
        this.registry = Objects.requireNonNull(registry, "registry cannot be null");
        this.supportedTypes = Set.copyOf(Objects.requireNonNull(supportedTypes, "supportedTypes cannot be null"));
        this.metricsByType = new ConcurrentHashMap<>();
        initializeMetrics();
    }

    /**
     * Initializes metrics for all supported resource types.
     * Creates and registers timers and counters with appropriate tags.
     */
    private void initializeMetrics() {
        supportedTypes.forEach(type -> {
            List<Tag> tags = List.of(Tag.of(EventMetricsConstants.Tags.RESOURCE_TYPE, type.name()));

            Timer latency = Timer.builder(EventMetricsConstants.Creation.LATENCY)
                                 .tags(tags)
                                 .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                                 .publishPercentileHistogram()
                                 .register(registry);

            Counter success = Counter.builder(EventMetricsConstants.Creation.TOTAL).tags(tags).register(registry);

            metricsByType.put(type, new MetricSet(latency, success, new ConcurrentHashMap<>()));
        });
    }

    @Override
    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    @Override
    public void recordCreationSuccess(Timer.Sample sample, ResourceType type) {
        Objects.requireNonNull(sample, "sample cannot be null");

        var metrics = getMetricsOrThrow(type);
        assert metrics != null;
        sample.stop(metrics.latency());
        metrics.success().increment();
    }

    @Override
    public void recordCreationError(Timer.Sample sample, ResourceType type, String errorType) {
        Objects.requireNonNull(sample, "sample cannot be null");
        Objects.requireNonNull(errorType, "errorType cannot be null");

        var metrics = getMetricsOrThrow(type);
        assert metrics != null;
        sample.stop(metrics.latency());

        metrics.errors()
               .computeIfAbsent(
                   errorType,
                   k -> Counter.builder(EventMetricsConstants.Creation.ERRORS)
                               .tags(
                                   List.of(
                                       Tag.of(EventMetricsConstants.Tags.RESOURCE_TYPE, type.name()),
                                       Tag.of(EventMetricsConstants.Tags.ERROR_TYPE, k)
                                   )
                               )
                               .register(registry)
               )
               .increment();
    }

    @Override
    public void close() {
        metricsByType.values().forEach(metrics -> {
            registry.remove(metrics.latency());
            registry.remove(metrics.success());
            metrics.errors().values().forEach(registry::remove);
        });
    }

    /**
     * Retrieves metrics for a given resource type with validation.
     *
     * @param type The resource type to get metrics for
     * @return MetricSet if type is supported, null otherwise
     * @throws NullPointerException if type is null
     */
    private MetricSet getMetricsOrThrow(ResourceType type) {
        Objects.requireNonNull(type, "type cannot be null");

        if (!supportedTypes.contains(type)) {
            log.warn("Metrics not tracked for unsupported resource type: {}", type);
            return null;
        }

        return metricsByType.get(type);
    }
}
