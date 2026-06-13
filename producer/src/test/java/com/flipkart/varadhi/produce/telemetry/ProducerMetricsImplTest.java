package com.flipkart.varadhi.produce.telemetry;

import com.flipkart.varadhi.produce.ProduceResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProducerMetricsImplTest {

    private static final String TOPIC = "project.topic";
    private static final String REGION = "local";

    @Test
    void received_IncrementsCountAndByteMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ProducerMetricsImpl metrics = new ProducerMetricsImpl(registry, TOPIC, REGION);

        metrics.received(64, 128);

        assertEquals(1.0, counter(registry, "producer.received.total.count"));
        assertEquals(128.0, counter(registry, "producer.received.total.bytes"));
        assertEquals(64.0, counter(registry, "producer.received.payload.bytes"));
    }

    @Test
    void accepted_Throttled_IncrementsEnforcedRejectionMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ProducerMetricsImpl metrics = new ProducerMetricsImpl(registry, TOPIC, REGION);

        metrics.accepted(ProduceResult.ofThrottled("msg-1"), null, 64);

        assertEquals(1.0, counter(registry, "producer.rejected.count", "false"));
        assertEquals(64.0, counter(registry, "producer.rejected.bytes", "false"));
    }

    @Test
    void shadowRejected_IncrementsShadowRejectionMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ProducerMetricsImpl metrics = new ProducerMetricsImpl(registry, TOPIC, REGION);

        metrics.shadowRejected(32);

        assertEquals(1.0, counter(registry, "producer.rejected.count", "true"));
        assertEquals(32.0, counter(registry, "producer.rejected.bytes", "true"));
    }

    @Test
    void close_RemovesTopicMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ProducerMetricsImpl metrics = new ProducerMetricsImpl(registry, TOPIC, REGION);
        metrics.received(1, 1);

        metrics.close();

        assertEquals(0, registry.find("producer.received.total.count").counters().size());
        assertEquals(0, registry.find("producer.received.total.bytes").counters().size());
    }

    private static double counter(SimpleMeterRegistry registry, String name) {
        return registry.get(name).counter().count();
    }

    private static double counter(SimpleMeterRegistry registry, String name, String shadow) {
        return registry.get(name).tag("shadow", shadow).counter().count();
    }
}
