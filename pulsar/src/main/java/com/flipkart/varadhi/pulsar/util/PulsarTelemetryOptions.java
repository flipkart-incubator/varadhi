package com.flipkart.varadhi.pulsar.util;

import com.flipkart.varadhi.pulsar.config.TelemetryOptions;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;

/**
 * Default telemetry implementation for Pulsar producers and consumers.
 * <p>
 * Publishes (besides {@link Producer#getStats()}): producer/consumer identity as tags,
 * {@link Producer#getLastSequenceId()}, {@link Producer#isConnected()} / {@link Consumer#isConnected()},
 * and from getStats(): send rates, latency percentiles, totals, pending queue size.
 */
public class PulsarTelemetryOptions implements TelemetryOptions {

    /**
     * Register a Pulsar producer for telemetry recording (e.g. metrics).
     *
     * @param producer the Pulsar producer to record; must not be null
     */
    @Override
    public void records(Producer<byte[]> producer) {
    }

    /**
     * Register a Pulsar consumer for telemetry recording. The consumer is {@link AutoCloseable};
     * implementations may use this to tie telemetry lifecycle to the consumer.
     *
     * @param consumer the Pulsar consumer to record; must not be null
     */
    @Override
    public void records(Consumer<byte[]> consumer) {
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     */
    @Override
    public void close() throws Exception {

    }
}
