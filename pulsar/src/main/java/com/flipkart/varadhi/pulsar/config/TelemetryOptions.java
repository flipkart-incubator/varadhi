package com.flipkart.varadhi.pulsar.config;

/**
 * Options for collecting and publishing telemetry (e.g. metrics) from Pulsar producers and consumers.
 * Implementations can register producers and consumers for recording and must release any resources
 * in {@link #close()}.
 */
public interface TelemetryOptions extends AutoCloseable {

    /**
     * Register a Pulsar producer for telemetry recording (e.g. metrics).
     *
     * @param producer the Pulsar producer to record; must not be null
     */
    void records(org.apache.pulsar.client.api.Producer<byte[]> producer);

    /**
     * Register a Pulsar consumer for telemetry recording. The consumer is {@link AutoCloseable};
     * implementations may use this to tie telemetry lifecycle to the consumer.
     *
     * @param consumer the Pulsar consumer to record; must not be null
     */
    void records(org.apache.pulsar.client.api.Consumer<byte[]> consumer);
}
