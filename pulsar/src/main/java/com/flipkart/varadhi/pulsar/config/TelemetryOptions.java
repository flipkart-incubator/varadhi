package com.flipkart.varadhi.pulsar.config;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Consumer;

/**
 * Options for collecting and publishing telemetry (e.g. metrics) from Pulsar producers and consumers.
 * Implementations can register producers and consumers for recording and must release any resources
 * in {@link #close()}.
 */
public interface TelemetryOptions extends AutoCloseable {

    /**
     * Register a Pulsar producer for telemetry recording (e.g. metrics).
     *
     * @param producer the Pulsar producer to recordTelemetry; must not be null
     */
    void recordTelemetry(Producer<byte[]> producer);

    /**
     * Register a Pulsar consumer for telemetry recording. The consumer is {@link AutoCloseable};
     * implementations may use this to tie telemetry lifecycle to the consumer.
     *
     * @param consumer the Pulsar consumer to recordTelemetry; must not be null
     */
    void recordTelemetry(Consumer<byte[]> consumer);
}
