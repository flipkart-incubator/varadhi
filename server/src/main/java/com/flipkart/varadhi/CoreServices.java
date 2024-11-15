package com.flipkart.varadhi;


import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.utils.JsonMapper;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ServiceAttributes;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.utils.LoaderUtils.loadClass;

@Slf4j
@Getter
public class CoreServices {

    private final ConfigFileResolver configResolver;
    @Getter(AccessLevel.PRIVATE)
    private final ObservabilityStack observabilityStack;
    private final MessagingStackProvider messagingStackProvider;
    private final MetaStoreProvider metaStoreProvider;

    public CoreServices(AppConfiguration configuration, ConfigFileResolver configResolver) {
        this.configResolver = configResolver;
        this.observabilityStack = setupObservabilityStack(configuration);
        this.messagingStackProvider = setupMessagingStackProvider(configuration.getMessagingStackOptions());
        this.metaStoreProvider = setupMetaStoreProvider(configuration.getMetaStoreOptions());
    }


    public Tracer getTracer(String instrumentationScope) {
        return this.observabilityStack.getOpenTelemetry().getTracer(instrumentationScope);
    }

    public MeterRegistry getMeterRegistry() {
        return this.observabilityStack.getMeterRegistry();
    }

    public OpenTelemetry getOpenTelemetry() {
        return this.observabilityStack.getOpenTelemetry();
    }

    /*
      TODO::RouteProvider needs to be fixed.
       - Should be Strongly typed instead of Raw.
       - Also it should be injected dynamically.
     */

    private MetaStoreProvider setupMetaStoreProvider(MetaStoreOptions metaStoreOptions) {
        MetaStoreProvider provider = loadClass(metaStoreOptions.getProviderClassName());
        provider.init(metaStoreOptions);
        return provider;
    }

    private MessagingStackProvider setupMessagingStackProvider(MessagingStackOptions messagingStackOptions) {
        MessagingStackProvider provider = loadClass(messagingStackOptions.getProviderClassName());
        provider.init(messagingStackOptions, JsonMapper.getMapper());
        return provider;
    }

    private ObservabilityStack setupObservabilityStack(AppConfiguration configuration) {

        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ServiceAttributes.SERVICE_NAME, "com.flipkart.varadhi")));

        // TODO: make tracing togglable and configurable.
        float sampleRatio = 1.0f;

        // exporting spans as logs, but can be replaced with otlp exporter.
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(LoggingSpanExporter.create()).build())
                .setResource(resource)
                .setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(sampleRatio)))
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        // TODO: make meter registry config configurable. each registry comes with its own config.
        String meterExporter = "jmx";
        MeterRegistry meterRegistry = switch (meterExporter) {
            case "jmx" -> new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
            case "prometheus" -> new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            case "otlp" -> new OtlpMeterRegistry(configuration.getOtelOptions()::get, Clock.SYSTEM);
            default -> null;
        };
        return new ObservabilityStack(openTelemetry, meterRegistry);
    }


    @Getter
    @AllArgsConstructor
    public static class ObservabilityStack {
        private final OpenTelemetry openTelemetry;
        private final MeterRegistry meterRegistry;
    }


}
