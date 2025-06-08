package com.flipkart.varadhi.produce.telemetry;

import com.flipkart.varadhi.produce.ProduceResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;


/**
 * Implementation of {@link ProducerMetrics} that records metrics using Micrometer.
 * This class manages various metrics including message counts, latencies, and throughput rates
 * for both successful and failed message productions.
 *
 * <p>The metrics recorded include:
 * <ul>
 *   <li> Received messages: Counts the total number of messages received, including their sizes.</li>
 *   <li> Filtered messages: Counts messages that were filtered out by the system.</li>
 *   <li> Successes: Records successful message productions along with their latencies.</li>
 *   <li> Failures: Records failed message productions, categorizing them by error type and latency.</li>
 * </ul>
 *
 * The latency metrics are not recorded per topic. That should be taken care of at the api layer. These latencies
 * are for tracking messaging stack performance and are not specific to any topic.
 *
 * <p>Thread-safety is ensured through the use of atomic counters and thread-safe Micrometer components.
 *
 * @see ProducerMetrics
 */
@Slf4j
public final class ProducerMetricsImpl implements ProducerMetrics {

    private final MeterRegistry registry;
    private final Counter receivedTotalBytes;
    private final Counter receivedPayloadBytes;
    private final Counter filteredCount;
    private final Counter rejectedCount;
    private final Timer successTimer;
    private final Timer failureTimer;

    public ProducerMetricsImpl(MeterRegistry registry, String topicFQN) {
        this.registry = registry;
        this.receivedTotalBytes = Counter.builder("producer.received.total.bytes")
                                         .tag("topic", topicFQN)
                                         .register(registry);
        this.receivedPayloadBytes = Counter.builder("producer.received.payload.bytes")
                                           .tag("topic", topicFQN)
                                           .register(registry);
        this.filteredCount = Counter.builder("producer.filtered.count").tag("topic", topicFQN).register(registry);
        this.rejectedCount = Counter.builder("producer.rejected.count").tag("topic", topicFQN).register(registry);

        // relying on the registry to dedup it for other ProducerMetricsImpl instances. Since instance creation is rare
        // operation, this is ok.
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
        receivedPayloadBytes.increment(payloadSizeBytes);
        receivedTotalBytes.increment(msgSizeBytes);
    }

    @Override
    public void accepted(ProduceResult result, Throwable t) {
        if (t != null) {
            // can ignore this, at least from the metrics pov. The api layer will handle it.
            return;
        }

        switch (result.getProduceStatus()) {
            case Success -> successTimer.record(result.getLatencyMs(), TimeUnit.MILLISECONDS);
            case Filtered -> filteredCount.increment();
            case Throttled, Blocked, NotAllowed -> rejectedCount.increment();
            case Failed -> failureTimer.record(result.getLatencyMs(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        registry.remove(receivedPayloadBytes);
        registry.remove(receivedTotalBytes);
        registry.remove(filteredCount);
    }
}
