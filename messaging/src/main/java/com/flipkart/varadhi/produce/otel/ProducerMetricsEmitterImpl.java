package com.flipkart.varadhi.produce.otel;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.flipkart.varadhi.Constants.Meters.Produce.BYTES_METER;
import static com.flipkart.varadhi.Constants.Meters.Produce.LATENCY_METER;
import static com.flipkart.varadhi.Constants.Tags.*;

public class ProducerMetricsEmitterImpl implements ProducerMetricsEmitter {
    private final MeterRegistry meterRegistry;
    private final Map<String, String> produceAttributes;
    private final int messageSize;

    public ProducerMetricsEmitterImpl(
            MeterRegistry meterRegistry, int messageSize, Map<String, String> produceAttributes
    ) {
        this.meterRegistry = meterRegistry;
        this.messageSize = messageSize;
        this.produceAttributes = produceAttributes;
    }

    @Override
    public void emit(boolean succeeded, long producerLatency) {
        List<Tag> tags = getTags(produceAttributes);
        getProducedBytesCounter(tags).increment(messageSize);
        tags.add(Tag.of(TAG_PRODUCE_RESULT, succeeded ? TAG_VALUE_RESULT_SUCCESS : TAG_VALUE_RESULT_FAILED));
        getLatencyTimer(tags).record(producerLatency, TimeUnit.MILLISECONDS);
    }

    private List<Tag> getTags(Map<String, String> produceAttributes) {
        // TODO:: All of them needed, can this be driven from config ?
        List<Tag> tags = new ArrayList<>();
        produceAttributes.forEach((k, v) -> tags.add(Tag.of(k, v)));
        return tags;
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

