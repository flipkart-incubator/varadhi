package com.flipkart.varadhi;

import com.flipkart.varadhi.cluster.ClusterManager;
import com.flipkart.varadhi.cluster.impl.ClusterManagerImpl;
import com.flipkart.varadhi.components.Component;
import com.flipkart.varadhi.components.ComponentKind;
import com.flipkart.varadhi.components.controller.Controller;
import com.flipkart.varadhi.components.server.Server;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.utils.HostUtils;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class VaradhiApplication {

    public static void main(String[] args) {

        try {
            String hostName = HostUtils.getHostName();
            log.info("VaradhiApplication Starting on {}.", hostName);
            AppConfiguration configuration = readConfiguration(args);
            CoreServices services = new CoreServices(configuration);
            Vertx vertx = createVertex(configuration, services);
            ClusterManager clusterManager = createClusterManager(vertx);
            Map<ComponentKind, Component> components = getComponents(configuration, services);

            Future.all(components.entrySet().stream().map(e -> e.getValue().start(vertx, clusterManager).onComplete(ar -> {
                if (ar.succeeded()) {
                    log.info("Component({}) started.", e.getKey());
                } else {
                    log.error("Component({}) failed to start.", e.getKey(), ar.cause());
                }
            })).collect(Collectors.toList())).onComplete(ar -> {
                if (ar.succeeded()) {
                    log.info("VaradhiApplication Started on {}.", hostName);
                } else {
                    log.error("VaradhiApplication failed to start.", ar.cause());
                }
            });
        } catch (Exception e) {
            log.error("Failed to initialise the VaradhiApplication.", e);
            System.exit(-1);
        }

        // TODO: check need for shutdown hook
//        Runtime.getRuntime().addShutdownHook();
    }

    private static Vertx createVertex(AppConfiguration configuration, CoreServices services) {
        log.debug("Creating Vertex");
        VertxOptions vertxOptions = configuration.getVertxOptions()
                .setTracingOptions(new OpenTelemetryOptions(services.getOpenTelemetry()))
                .setMetricsOptions(new MicrometerMetricsOptions()
                        .setMicrometerRegistry(services.getMeterRegistry())
                        .setRegistryName("default")
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true));
        Vertx vertx = Vertx.vertx(vertxOptions);
        log.debug("Created Vertex");
        return vertx;
    }

    private static ClusterManager createClusterManager(Vertx vertx) {
        // TODO:: Placeholder for now. This node joining the cluster needs to be closed
        // along with ClusterManager related changes.
        return new ClusterManagerImpl(vertx);
    }

    public static AppConfiguration readConfiguration(String[] args) {
        if (args.length < 1) {
            log.error("Usage: java com.flipkart.varadhi.VaradhiApplication configuration.yml");
            System.exit(-1);
        }
        return readConfigFromFile(args[0]);
    }

    public static AppConfiguration readConfigFromFile(String filePath) throws InvalidConfigException {
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
            return content.mapTo(AppConfiguration.class);
        } catch (Exception e) {
            throw new InvalidConfigException("Failed to load Application Configuration", e);
        } finally {
            retriever.close();
            vertx.close();
        }
    }

    private static Map<ComponentKind, Component> getComponents(
            AppConfiguration configuration, CoreServices coreServices
    ) {
        //TODO:: check if there is need for ordered sequence of component.
        return Arrays.stream(ComponentKind.values())
                .filter(kind -> !kind.equals(ComponentKind.All) && (
                        configuration.getComponents().contains(ComponentKind.All) ||
                                configuration.getComponents().contains(kind)
                ))
                .collect(Collectors.toMap(Function.identity(), kind -> switch (kind) {
                    case Server -> new Server(configuration, coreServices);
                    case Controller -> new Controller(configuration, coreServices);
                    default -> throw new IllegalArgumentException("Unknown Component Kind: " + kind);
                }));
    }
}
