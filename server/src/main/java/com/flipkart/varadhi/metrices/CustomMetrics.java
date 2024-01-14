package com.flipkart.varadhi.metrices;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.HttpServerMetrics;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistry;
import io.vertx.micrometer.impl.VertxMetricsImpl;

import java.util.concurrent.ConcurrentMap;

public class CustomMetrics extends VertxMetricsImpl  {

    public final MeterRegistry meterRegistry;
    public CustomMetrics(
            MicrometerMetricsOptions options, BackendRegistry backendRegistry,
            ConcurrentMap<Meter.Id, Object> gaugesTable
    ) {
        super(options, backendRegistry, gaugesTable);
        this.meterRegistry = backendRegistry.getMeterRegistry();
    }


    @Override
    public HttpServerMetrics<?, ?, ?> createHttpServerMetrics(HttpServerOptions options, SocketAddress localAddress) {
        return new CustomHttpServerMetrics(meterRegistry);
    }
}
