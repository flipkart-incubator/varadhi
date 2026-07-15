package com.flipkart.varadhi.produce.telemetry;

import com.flipkart.varadhi.produce.ProduceResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Micrometer-backed {@link ProducerMetrics} with per-topic tags ({@code topic}, {@code project},
 * {@code region}).
 */
@Slf4j
public final class ProducerMetricsImpl implements ProducerMetrics {

    private final MeterRegistry registry;
    private final List<io.micrometer.core.instrument.Meter> registeredMeters = new ArrayList<>();

    private final Counter receivedTotalCount;
    private final Counter receivedTotalBytes;
    private final Counter receivedPayloadBytes;
    private final Counter filteredCount;
    private final Counter rejectedCount;
    private final Counter rejectedBytes;
    private final Counter shadowRejectedCount;
    private final Counter shadowRejectedBytes;
    private final Timer successTimer;
    private final Timer failureTimer;

    public ProducerMetricsImpl(MeterRegistry registry, String topicFQN, String region) {
        this(registry, topicFQN, region, projectFromTopicFqn(topicFQN));
    }

    public ProducerMetricsImpl(MeterRegistry registry, String topicFQN, String region, String project) {
        this.registry = registry;
        var tags = io.micrometer.core.instrument.Tags.of("topic", topicFQN, "project", project, "region", region);
        var enforcedTags = tags.and("shadow", "false");
        var shadowTags = tags.and("shadow", "true");

        this.receivedTotalCount = registerCounter("producer.received.total.count", tags);
        this.receivedTotalBytes = registerCounter("producer.received.total.bytes", tags);
        this.receivedPayloadBytes = registerCounter("producer.received.payload.bytes", tags);
        this.filteredCount = registerCounter("producer.filtered.count", tags);
        this.rejectedCount = registerCounter("producer.rejected.count", enforcedTags);
        this.rejectedBytes = registerCounter("producer.rejected.bytes", enforcedTags);
        this.shadowRejectedCount = registerCounter("producer.rejected.count", shadowTags);
        this.shadowRejectedBytes = registerCounter("producer.rejected.bytes", shadowTags);

        this.successTimer = Timer.builder("producer.latency")
                                 .tag("result", "success")
                                 .publishPercentileHistogram()
                                 .register(registry);
        this.failureTimer = Timer.builder("producer.latency")
                                 .tag("result", "failure")
                                 .publishPercentileHistogram()
                                 .register(registry);
    }

    @Override
    public void received(int payloadSizeBytes, int msgSizeBytes) {
        receivedTotalCount.increment();
        receivedPayloadBytes.increment(payloadSizeBytes);
        receivedTotalBytes.increment(msgSizeBytes);
    }

    @Override
    public void accepted(ProduceResult result, Throwable t, long messageBytes) {
        if (t != null) {
            return;
        }

        switch (result.getProduceStatus()) {
            case Success -> successTimer.record(result.getLatencyMs(), TimeUnit.MILLISECONDS);
            case Filtered -> filteredCount.increment();
            case Throttled, Blocked, NotAllowed -> rejected(messageBytes, false);
            case Failed -> failureTimer.record(result.getLatencyMs(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void shadowRejected(long messageBytes) {
        rejected(messageBytes, true);
    }

    @Override
    public void close() {
        registeredMeters.forEach(registry::remove);
        registeredMeters.clear();
    }

    private void rejected(long messageBytes, boolean shadow) {
        if (shadow) {
            shadowRejectedCount.increment();
            shadowRejectedBytes.increment(messageBytes);
        } else {
            rejectedCount.increment();
            rejectedBytes.increment(messageBytes);
        }
    }

    private Counter registerCounter(String name, io.micrometer.core.instrument.Tags tags) {
        return register(Counter.builder(name).tags(tags).register(registry));
    }

    private <T extends io.micrometer.core.instrument.Meter> T register(T meter) {
        registeredMeters.add(meter);
        return meter;
    }

    private static String projectFromTopicFqn(String topicFqn) {
        int dot = topicFqn.indexOf('.');
        return dot < 0 ? topicFqn : topicFqn.substring(0, dot);
    }
}
