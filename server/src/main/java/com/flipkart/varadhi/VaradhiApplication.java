package com.flipkart.varadhi;

import com.flipkart.varadhi.verticles.webserver.WebServerVerticle;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.custom.VaradhiZkClusterManager;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.verticles.controller.ControllerVerticle;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.MemberConfig;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import com.flipkart.varadhi.utils.HostUtils;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MetricsNaming;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.Arrays;
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
            CoreServices services = new CoreServices(configuration);
            VaradhiZkClusterManager clusterManager = getClusterManager(configuration, host);
            Map<ComponentKind, Verticle> verticles = getComponentVerticles(configuration, services, clusterManager);
            createClusteredVertx(configuration, clusterManager, services, host).compose(vertx ->
                            Future.all(verticles.entrySet().stream()
                                    .map(es -> vertx.deployVerticle(es.getValue()).onComplete(ar -> {
                                        if (ar.succeeded()) {
                                            log.info("component: {} started.", es.getKey());
                                        } else {
                                            log.error("component: {} failed to start. {}", es.getKey(), ar.cause());
                                        }
                                    })).collect(Collectors.toList()))
                    )
                    .onSuccess(ar -> log.info("VaradhiApplication Started on {}.", host))
                    .onFailure(t -> {
                        log.error("VaradhiApplication on host {} failed to start. {} ", host, t);
                        log.error("Closing the application.");
                        System.exit(-1);
                    });
        } catch (Exception e) {
            log.error("Failed to initialise the VaradhiApplication.", e);
            log.error("Closing the application.");
            System.exit(-1);
        }
        // TODO: check need for shutdown hook
    }



    private static VaradhiZkClusterManager getClusterManager(AppConfiguration config, String host) {
        CuratorFramework curatorFramework = CuratorFrameworkCreator.create(config.getZookeeperOptions());
        DeliveryOptions deliveryOptions = new DeliveryOptions();
        deliveryOptions.setTracingPolicy(config.getDeliveryOptions().getTracingPolicy());
        deliveryOptions.setSendTimeout(config.getDeliveryOptions().getTimeoutMs());
        return new VaradhiZkClusterManager(curatorFramework, deliveryOptions, host);
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
                new MemberInfo(host, port, config.getRoles(), config.getCpuCount(), config.getNicMBps());
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

    private static Map<ComponentKind, Verticle> getComponentVerticles(
            AppConfiguration config, CoreServices coreServices, VaradhiClusterManager clusterManager
    ) {
        return Arrays.stream(config.getMember().getRoles()).distinct()
                .collect(Collectors.toMap(Function.identity(), kind -> switch (kind) {
                    case Server -> new WebServerVerticle(config, coreServices, clusterManager);
                    case Controller -> new ControllerVerticle(config, coreServices, clusterManager);
                }));
    }
}
