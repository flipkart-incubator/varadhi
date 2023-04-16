package com.flipkart.varadhi;

import com.flipkart.varadhi.configs.ServerConfiguration;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server {

    public static void main(String[] args) throws Exception {
        ServerConfiguration configuration = readConfiguration(args);
        OpenTelemetry openTelemetry = setupOpenTelemetry(configuration);

        log.info("Server Starting.");
        Vertx vertx =
                Vertx.vertx(configuration.getVertxOptions().setTracingOptions(new OpenTelemetryOptions(openTelemetry)));

        CoreServices services = new CoreServices(openTelemetry, vertx, configuration);

        vertx.deployVerticle(() -> new RestVerticle(configuration, services), configuration.getDeploymentOptions());

        // TODO: check need for shutdown hook
    }

    public static OpenTelemetry setupOpenTelemetry(ServerConfiguration configuration) {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "com.flipkart.varadhi")));

        // TODO: make tracing togglable and configurable.
        float sampleRatio = 1.0f;

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(LoggingSpanExporter.create()))
                .setResource(resource)
                .setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(sampleRatio)))
                .build();

//        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
//                .registerMetricReader(PeriodicMetricReader.builder(OtlpGrpcMetricExporter.builder().build()).build())
//                .setResource(resource)
//                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
//                .setMeterProvider(sdkMeterProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        return openTelemetry;
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
