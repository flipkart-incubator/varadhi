package com.flipkart.varadhi.metrices;

import com.flipkart.varadhi.utils.MetricsUtil;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.micrometer.backends.BackendRegistry;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

public class CustomMetricsFactory implements VertxMetricsFactory {

    private static final Map<MeterRegistry, ConcurrentHashMap<Meter.Id, Object>> tables = new WeakHashMap<>(1);
    private final MeterRegistry meterRegistry;

    public CustomMetricsFactory(
            MeterRegistry meterRegistry
    ) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public VertxMetrics metrics(VertxOptions vertxOptions) {

        MicrometerMetricsOptions options = new MicrometerMetricsOptions()
                .setMicrometerRegistry(meterRegistry)
                .setRegistryName("default")
                .setJvmMetricsEnabled(true)
                .setClientRequestTagsProvider(httpRequest -> MetricsUtil.getCustomHttpHeaders(httpRequest.headers()))
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setEnabled(true);

        BackendRegistry backendRegistry = BackendRegistries.setupBackend(options);
        ConcurrentHashMap<Meter.Id, Object> gaugesTable;
        synchronized (tables) {
            gaugesTable = tables.computeIfAbsent(backendRegistry.getMeterRegistry(),
                    meterRegistry -> new ConcurrentHashMap<>()
            );
        }
        CustomMetricsImpl metrics = new CustomMetricsImpl(options, backendRegistry, gaugesTable);
        backendRegistry.init();

        if (options.isJvmMetricsEnabled()) {
            new ClassLoaderMetrics().bindTo(backendRegistry.getMeterRegistry());
            new JvmMemoryMetrics().bindTo(backendRegistry.getMeterRegistry());
            new JvmGcMetrics().bindTo(backendRegistry.getMeterRegistry());
            new ProcessorMetrics().bindTo(backendRegistry.getMeterRegistry());
            new JvmThreadMetrics().bindTo(backendRegistry.getMeterRegistry());
        }

        return metrics;
    }
}
