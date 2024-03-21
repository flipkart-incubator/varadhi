package com.flipkart.varadhi;


import com.flipkart.varadhi.cluster.MemberInfo;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.custom.VaradhiZkClusterManager;
import com.flipkart.varadhi.components.Component;
import com.flipkart.varadhi.components.ComponentKind;
import com.flipkart.varadhi.components.controller.Controller;
import com.flipkart.varadhi.components.webserver.WebServer;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.MemberConfig;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.utils.HostUtils;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MetricsNaming;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class VaradhiApplication {

    public static void main(String[] args) {

        try {
            String host = HostUtils.getHostName();
            log.info("VaradhiApplication Starting on {}.", host);
            AppConfiguration configuration = readConfiguration(args);
            String clusterNodeId = configuration.getMember().getNodeId();
            CoreServices services = new CoreServices(configuration);

            VaradhiZkClusterManager
                    clusterManager = new VaradhiZkClusterManager(configuration.getZookeeperOptions(), clusterNodeId);

            Map<ComponentKind, Component> components = getComponents(configuration, services, clusterManager);

            createClusteredVertx(configuration, clusterManager, services, "127.0.0.1").compose(vertx ->
                    Future.all(components.entrySet().stream()
                            .map(es -> es.getValue().start(vertx).onComplete(ar -> {
                                if (ar.succeeded()) {
                                    log.info("component: {} started.", es.getKey());
                                } else {
                                    log.error("component: {} failed to start. {}", es.getKey(), ar.cause());
                                }
                            })).collect(Collectors.toList()))
            ).onComplete(ar -> {
                if (ar.succeeded()) {
                    log.info("VaradhiApplication Started on {}.", host);
                } else {
                    log.error("VaradhiApplication on host {} failed to start. {} ", host, ar.cause());
                }
            });
        } catch (Exception e) {
            log.error("Failed to initialise the VaradhiApplication.", e);
            System.exit(-1);
        }
        // TODO: check need for shutdown hook
    }

    private static Future<Vertx> createClusteredVertx(
            AppConfiguration config, ClusterManager clusterManager, CoreServices services, String host
    ) {
        int port = 0;
        EventBusOptions eventBusOptions = new EventBusOptions()
                .setHost(host)
                .setPort(port)
                .setClusterNodeMetadata(getMemberInfoAsJson(config.getMember(), host, port));

        VertxOptions vertxOptions = config.getVertxOptions()
                .setTracingOptions(new OpenTelemetryOptions(services.getOpenTelemetry()))
                .setMetricsOptions(new MicrometerMetricsOptions()
                        .setMicrometerRegistry(services.getMeterRegistry())
                        .setMetricsNaming(MetricsNaming.v4Names())
                        .setRegistryName("default")
                        .addDisabledMetricsCategory(MetricsDomain.HTTP_SERVER)
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true))
                .setEventBusOptions(eventBusOptions);

        return Vertx.builder().with(vertxOptions).withClusterManager(clusterManager).buildClustered();
    }

    private static JsonObject getMemberInfoAsJson(MemberConfig config, String host, int port) {
        MemberInfo info =
                new MemberInfo(config.getNodeId(), host, port, config.getRoles(), config.getCpuCount(), config.getNicMBps());
        return JsonObject.mapFrom(info);
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
            AppConfiguration config, CoreServices coreServices, VaradhiClusterManager clusterManager
    ) {
        List<ComponentKind> configuredComponents = Arrays.stream(config.getMember().getRoles()).toList();
        //TODO:: check if there is need for ordered sequence of component.
        return Arrays.stream(ComponentKind.values())
                .filter(kind -> !kind.equals(ComponentKind.All) && (
                        configuredComponents.contains(ComponentKind.All) || configuredComponents.contains(kind)
                ))
                .collect(Collectors.toMap(Function.identity(), kind -> switch (kind) {
                    case Server -> new WebServer(config, coreServices, clusterManager);
                    case Controller -> new Controller(config, coreServices, clusterManager);
                    default -> throw new IllegalArgumentException("Unknown Component Kind: " + kind);
                }));
    }
}
