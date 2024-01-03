package com.flipkart.varadhi.metrices;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.HttpServerMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;

public class CustomMetrics implements VertxMetrics {

    private final MeterRegistry meterRegistry;

    public CustomMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public HttpServerMetrics<?, ?, ?> createHttpServerMetrics(HttpServerOptions options, SocketAddress localAddress) {
        return new CustomHttpServerMetrics(meterRegistry);
    }
}
