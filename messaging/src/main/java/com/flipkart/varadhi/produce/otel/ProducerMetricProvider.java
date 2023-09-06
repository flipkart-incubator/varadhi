package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.entities.ProduceContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.flipkart.varadhi.Constants.Meters.Produce.BYTES_METER;
import static com.flipkart.varadhi.Constants.Meters.Produce.LATENCY_METER;
import static com.flipkart.varadhi.Constants.Tags.*;

public class ProducerMetricProvider {
    private final MeterRegistry meterRegistry;
    private boolean enabled;

    public ProducerMetricProvider(boolean enabled, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.enabled = enabled;
    }

    public void OnMessageProduced(boolean succeeded, long producerLatency, ProduceContext context) {
        if (enabled) {
            List<Tag> tags = getTags(context);
            getProducedBytesCounter(tags).increment(context.getRequestContext().getBytesReceived());
            tags.add(Tag.of(TAG_NAME_PRODUCE_RESULT, succeeded ? TAG_VALUE_RESULT_SUCCESS : TAG_VALUE_RESULT_FAILED));
            getLatencyTimer(tags).record(producerLatency, TimeUnit.MILLISECONDS);
        }
    }

    private List<Tag> getTags(ProduceContext context) {
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.of(TAG_NAME_REGION, context.getTopicContext().getRegion()));
        tags.add(Tag.of(TAG_NAME_PROJECT, context.getTopicContext().getProjectName()));
        tags.add(Tag.of(TAG_NAME_TOPIC, context.getTopicContext().getTopicName()));
        tags.add(Tag.of(TAG_NAME_IDENTITY, context.getRequestContext().getProduceIdentity()));
        tags.add(Tag.of(TAG_NAME_HOST, context.getRequestContext().getServiceHost()));
        return tags;
    }

    private Counter getProducedBytesCounter(List<Tag> tags) {
        // meter name - "produce.bytes", tags- region, project, topic, host
        return Counter.builder(BYTES_METER).tags(tags).register(meterRegistry);
    }

    private Timer getLatencyTimer(List<Tag> tags) {
        // meter name - "produce.latency", tags- region, project, topic, host, result
        return Timer.builder(LATENCY_METER).tags(tags).publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .publishPercentileHistogram().register(meterRegistry);
    }
}

