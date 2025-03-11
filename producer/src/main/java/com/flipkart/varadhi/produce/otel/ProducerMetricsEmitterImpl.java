package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.produce.config.ProducerErrorType;
import com.flipkart.varadhi.produce.config.ProducerMetricsConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link ProducerMetricsEmitter} that records metrics using Micrometer.
 * This class manages various metrics including message counts, latencies, and throughput rates
 * for both successful and failed message productions.
 *
 * <p>The metrics recorded include:
 * <ul>
 *   <li>Failed message counts by error type</li>
 *   <li>End-to-end latency distribution</li>
 *   <li>Storage latency distribution</li>
 *   <li>Message size distribution</li>
 *   <li>Message and byte throughput rates</li>
 * </ul>
 *
 * <p>Thread-safety is ensured through the use of atomic counters and thread-safe Micrometer components.
 *
 * @see ProducerMetricsEmitter
 * @see ProducerMetricsConfig
 */
@Slf4j
public final class ProducerMetricsEmitterImpl implements ProducerMetricsEmitter {
    private static final String METRIC_PREFIX = "varadhi.producer.messages.";
    private static final String TAG_FILTERED = "filtered";
    private static final String TAG_ERROR_TYPE = "error_type";
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final MeterRegistry meterRegistry;
    private final ProducerMetricsConfig config;
    private final ScheduledExecutorService scheduler;

    private Map<ProducerErrorType, Counter> failedMessagesCounters;
    private Timer e2eLatencyTimer;
    private Timer storageLatencyTimer;
    private DistributionSummary messageSizeDistribution;
    private AtomicLong messageCounter;
    private AtomicLong byteCounter;
    private AtomicLong filteredMessageCounter;
    private AtomicLong filteredByteCounter;

    /**
     * Constructs a new ProducerMetricsEmitterImpl with the specified configuration.
     *
     * @param meterRegistry     the registry for recording metrics
     * @param config            configuration for metrics collection
     * @param produceAttributes attributes to be included as tags with metrics
     */
    public ProducerMetricsEmitterImpl(
        MeterRegistry meterRegistry,
        ProducerMetricsConfig config,
        Map<String, String> produceAttributes
    ) {
        this.meterRegistry = meterRegistry;
        this.config = config;
        this.scheduler = createScheduler();

        List<Tag> baseTags = getTags(produceAttributes);
        initializeMetrics(baseTags);
        scheduleThroughputReset();
    }

    private ScheduledExecutorService createScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "producer-metrics-resetter");
            thread.setDaemon(true);
            return thread;
        });
    }

    private void initializeMetrics(List<Tag> baseTags) {
        initializeFailedMessageCounters(baseTags);
        initializeLatencyTimers(baseTags);
        initializeSizeDistribution(baseTags);
        initializeThroughputCounters();
        registerThroughputGauges(baseTags);
    }

    private void initializeFailedMessageCounters(List<Tag> baseTags) {
        this.failedMessagesCounters = new EnumMap<>(ProducerErrorType.class);
        for (ProducerErrorType errorType : ProducerErrorType.values()) {
            List<Tag> errorTags = new ArrayList<>(baseTags);
            errorTags.add(Tag.of(TAG_ERROR_TYPE, errorType.getValue()));
            failedMessagesCounters.put(errorType, Counter.builder(METRIC_PREFIX + "failed")
                    .tags(errorTags)
                    .description("Number of failed message productions")
                    .register(meterRegistry));
        }
    }

    private void initializeLatencyTimers(List<Tag> baseTags) {
        this.e2eLatencyTimer = Timer.builder(METRIC_PREFIX + "latency")
                .tags(baseTags)
                .description("End-to-end message production latency")
                .publishPercentiles(config.getLatencyPercentiles())
                .publishPercentileHistogram(config.isEnableHistogram())
                .register(meterRegistry);

        this.storageLatencyTimer = Timer.builder(METRIC_PREFIX + "latency.storage")
                .tags(baseTags)
                .description("Storage write latency")
                .publishPercentiles(config.getLatencyPercentiles())
                .publishPercentileHistogram(config.isEnableHistogram())
                .register(meterRegistry);
    }

    private void initializeSizeDistribution(List<Tag> baseTags) {
        this.messageSizeDistribution = DistributionSummary.builder(METRIC_PREFIX + "size")
                .tags(baseTags)
                .description("Message size distribution")
                .baseUnit("bytes")
                .register(meterRegistry);
    }

    private void initializeThroughputCounters() {
        this.messageCounter = new AtomicLong(0);
        this.byteCounter = new AtomicLong(0);
        this.filteredMessageCounter = new AtomicLong(0);
        this.filteredByteCounter = new AtomicLong(0);
    }

    private void registerThroughputGauges(List<Tag> baseTags) {
        // Message throughput gauges
        registerMessageThroughputGauge(baseTags, false, messageCounter);
        registerMessageThroughputGauge(baseTags, true, filteredMessageCounter);

        // Byte throughput gauges
        registerByteThroughputGauge(baseTags, false, byteCounter);
        registerByteThroughputGauge(baseTags, true, filteredByteCounter);
    }

    private void registerMessageThroughputGauge(List<Tag> baseTags, boolean filtered, AtomicLong counter) {
        Gauge.builder(METRIC_PREFIX + "throughput.messages", counter, this::calculateMessageRate)
                .tags(baseTags)
                .tag(TAG_FILTERED, String.valueOf(filtered))
                .description(filtered ? "Filtered message production rate" : "Message production rate")
                .register(meterRegistry);
    }

    private void registerByteThroughputGauge(List<Tag> baseTags, boolean filtered, AtomicLong counter) {
        Gauge.builder(METRIC_PREFIX + "throughput.bytes", counter, this::calculateByteRate)
                .tags(baseTags)
                .tag(TAG_FILTERED, String.valueOf(filtered))
                .description(filtered ? "Filtered byte production rate" : "Byte production rate")
                .register(meterRegistry);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if latencies are negative or message size is invalid
     */
    @Override
    public void emit(boolean succeeded, long producerLatency, long storageLatency,
                     int messageSize, boolean filtered, ProducerErrorType errorType) {
        validateMetricValues(producerLatency, storageLatency, messageSize);

        if (filtered) {
            recordFilteredMetrics(messageSize);
            return;
        }

        if (!succeeded) {
            recordFailureMetrics(errorType);
            return;
        }

        recordSuccessMetrics(producerLatency, storageLatency, messageSize);
    }

    private void validateMetricValues(long producerLatency, long storageLatency, int messageSize) {
        if (producerLatency < 0 || storageLatency < 0) {
            throw new IllegalArgumentException(
                    "Latency values must be non-negative. " +
                            "Producer latency: " + producerLatency +
                            ", Storage latency: " + storageLatency
            );
        }
        if (messageSize < 0) {
            throw new IllegalArgumentException("Message size must be non-negative: " + messageSize);
        }
    }

    private void recordFilteredMetrics(int messageSize) {
        filteredMessageCounter.incrementAndGet();
        filteredByteCounter.addAndGet(messageSize);
    }

    private void recordFailureMetrics(ProducerErrorType errorType) {
        Counter counter = failedMessagesCounters.get(errorType);
        if (counter != null) {
            counter.increment();
        } else {
            log.warn("Unknown error type encountered: {}", errorType);
            failedMessagesCounters.get(ProducerErrorType.INTERNAL).increment();
        }
    }

    private void recordSuccessMetrics(long producerLatency, long storageLatency, int messageSize) {
        messageCounter.incrementAndGet();
        byteCounter.addAndGet(messageSize);
        messageSizeDistribution.record(messageSize);
        e2eLatencyTimer.record(producerLatency, TimeUnit.MILLISECONDS);
        storageLatencyTimer.record(storageLatency, TimeUnit.MILLISECONDS);
    }

    private double calculateMessageRate(AtomicLong counter) {
        return calculateRate(counter);
    }

    private double calculateByteRate(AtomicLong counter) {
        return calculateRate(counter);
    }

    private double calculateRate(AtomicLong counter) {
        return (double) counter.get() / config.getThroughputRefreshInterval().toSeconds();
    }

    private void scheduleThroughputReset() {
        long intervalMillis = config.getThroughputRefreshInterval().toMillis();
        scheduler.scheduleAtFixedRate(
                this::resetCounters,
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    private void resetCounters() {
        messageCounter.set(0);
        byteCounter.set(0);
        filteredMessageCounter.set(0);
        filteredByteCounter.set(0);
    }

    private List<Tag> getTags(Map<String, String> produceAttributes) {
        // TODO:: All of them needed, can this be driven from config ?
        List<Tag> tags = new ArrayList<>();
        produceAttributes.forEach((k, v) -> tags.add(Tag.of(k, v)));
        return tags;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            if (scheduler != null) {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            log.warn("Interrupted while shutting down metrics scheduler", e);
        }
    }
}
