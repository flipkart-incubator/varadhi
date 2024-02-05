package com.flipkart.varadhi.produce.otel;

import com.flipkart.varadhi.core.entities.ApiContext;
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

public class ProducerMetricsImpl implements ProducerMetrics {
    private final MeterRegistry meterRegistry;

    public ProducerMetricsImpl(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void onMessageProduced(boolean succeeded, long producerLatency, ApiContext context) {
        List<Tag> tags = getTags(context);
        getProducedBytesCounter(tags).increment((int)context.get(ApiContext.BYTES_RECEIVED));
        tags.add(Tag.of(TAG_NAME_PRODUCE_RESULT, succeeded ? TAG_VALUE_RESULT_SUCCESS : TAG_VALUE_RESULT_FAILED));
        getLatencyTimer(tags).record(producerLatency, TimeUnit.MILLISECONDS);
    }

    private List<Tag> getTags(ApiContext context) {
        // TODO:: All of them needed, can this be driven from config ?
        List<Tag> tags = new ArrayList<>();
        addTag(context, TAG_NAME_REGION, ApiContext.REGION, tags);
        addTag(context, TAG_NAME_ORG, ApiContext.ORG, tags);
        addTag(context, TAG_NAME_TEAM, ApiContext.TEAM, tags);
        addTag(context, TAG_NAME_PROJECT, ApiContext.PROJECT, tags);
        addTag(context, TAG_NAME_TOPIC, ApiContext.TOPIC, tags);
        addTag(context, TAG_NAME_IDENTITY, ApiContext.IDENTITY, tags);
        addTag(context, TAG_NAME_HOST, ApiContext.SERVICE_HOST, tags);
        return tags;
    }

    private void addTag(ApiContext context, String tagName, String keyName, List<Tag> tags) {
        if (context.get(keyName) != null){
            tags.add(Tag.of(tagName, context.get(keyName)));
        }
    }

    private Counter getProducedBytesCounter(List<Tag> tags) {
        // meter name - "produce.bytes"
        return Counter.builder(BYTES_METER).tags(tags).register(meterRegistry);
    }

    private Timer getLatencyTimer(List<Tag> tags) {
        // meter name - "produce.latency"
        return Timer.builder(LATENCY_METER).tags(tags).publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .publishPercentileHistogram().register(meterRegistry);
    }
}

