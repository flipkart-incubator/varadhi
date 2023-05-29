package com.flipkart.varadhi;

import com.flipkart.varadhi.exceptions.InvalidConfigException;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server {

    public static void main(String[] args) {

        try {
            ServerConfiguration configuration = readConfiguration(args);
            CoreServices.ObservabilityStack observabilityStack = setupObservabilityStack(configuration);

            log.info("Server Starting.");
            VertxOptions vertxOptions = configuration.getVertxOptions()
                    .setTracingOptions(new OpenTelemetryOptions(observabilityStack.getOpenTelemetry()))
                    .setMetricsOptions(new MicrometerMetricsOptions()
                            .setMicrometerRegistry(observabilityStack.getMeterRegistry())
                            .setRegistryName("default")
                            .setJvmMetricsEnabled(true)
                            .setEnabled(true));


            Vertx vertx = Vertx.vertx(vertxOptions);

            CoreServices services = new CoreServices(observabilityStack, vertx, configuration);

            vertx.deployVerticle(
                    () -> new RestVerticle(configuration, services), configuration.getDeploymentOptions()
            ).onFailure(t -> {
                log.error("Could not start HttpServer verticle", t);
                System.exit(-1);
            });
        }catch (Exception e){
            log.error("Failed to initialise the server.", e);
            System.out.println("Failed to initialise the server:" + e);
            System.exit(-1);
        }
        // TODO: check need for shutdown hook
    }

    public static CoreServices.ObservabilityStack setupObservabilityStack(ServerConfiguration configuration) {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "com.flipkart.varadhi")));

        // TODO: make tracing togglable and configurable.
        float sampleRatio = 1.0f;

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(LoggingSpanExporter.create()).build())
                .setResource(resource)
                .setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(sampleRatio)))
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        // TODO: make meter registry config configurable.
        String meterExporter = "jmx";
        MeterRegistry meterRegistry = switch (meterExporter) {
            case "jmx" -> new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
            default -> new OtlpMeterRegistry();
        };

        return new CoreServices.ObservabilityStack(openTelemetry, meterRegistry);
    }

    public static ServerConfiguration readConfiguration(String[] args) {
        if (args.length < 1) {
            log.error("Usage: java com.flipkart.varadhi.Server configuration.yml");
            System.exit(-1);
        }
        return readConfigFromFile(args[0]);
    }

    public static ServerConfiguration readConfigFromFile(String filePath) throws InvalidConfigException {
        log.info("Loading Configuration.");
        Vertx vertx = Vertx.vertx();

        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(false)
                .setFormat("yaml")
                .setConfig(new JsonObject().put("path", filePath));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        try {
            JsonObject content = retriever.getConfig().toCompletionStage().toCompletableFuture().join();
            return content.mapTo(ServerConfiguration.class);
        } catch (Exception e) {
            throw new InvalidConfigException("Failed to load Application Configuration", e);
        } finally {
            retriever.close();
            vertx.close();
        }
    }
}
