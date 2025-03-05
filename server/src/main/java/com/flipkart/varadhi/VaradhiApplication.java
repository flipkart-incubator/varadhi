package com.flipkart.varadhi;

import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.custom.VaradhiZkClusterManager;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.common.reflect.RecursiveFieldUpdater;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import com.flipkart.varadhi.common.utils.HostUtils;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.MemberConfig;
import com.flipkart.varadhi.core.cluster.entities.ComponentKind;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.core.cluster.entities.NodeCapacity;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.spi.ConfigFile;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.verticles.consumer.ConsumerVerticle;
import com.flipkart.varadhi.verticles.controller.ControllerVerticle;
import com.flipkart.varadhi.verticles.webserver.WebServerVerticle;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class VaradhiApplication {
    public static void main(String[] args) {

        try {
            log.info("Starting VaradhiApplication");

            Pair<AppConfiguration, ConfigFileResolver> configReadResult = readConfiguration(args);
            AppConfiguration configuration = configReadResult.getLeft();
            ConfigFileResolver configResolver = configReadResult.getRight();

            HostUtils.init();
            StdHeaders.init(configuration.getMessageConfiguration().stdHeaders());

            MemberInfo memberInfo = getMemberInfo(configuration.getMember());
            CoreServices services = new CoreServices(configuration, configResolver);
            VaradhiZkClusterManager clusterManager = getClusterManager(configuration, memberInfo.hostname());
            Map<ComponentKind, Verticle> verticles = getComponentVerticles(
                configuration,
                services,
                clusterManager,
                memberInfo
            );
            createClusteredVertx(configuration, clusterManager, services, memberInfo).compose(
                vertx -> Future.all(
                    verticles.entrySet().stream().map(es -> vertx.deployVerticle(es.getValue()).onComplete(ar -> {
                        if (ar.succeeded()) {
                            log.info("component: {} started.", es.getKey());
                        } else {
                            log.error("component: {} failed to start.", es.getKey(), ar.cause());
                        }
                    })).collect(Collectors.toList())
                )
            ).onSuccess(ar -> log.info("VaradhiApplication Started on {}.", memberInfo.hostname())).onFailure(t -> {
                log.error("VaradhiApplication on host {} failed to start. {} ", memberInfo.hostname(), t);
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

    private static MemberInfo getMemberInfo(MemberConfig memberConfig) throws UnknownHostException {
        String hostName = HostUtils.getHostName();
        String hostAddress = HostUtils.getHostAddress();
        int networkKBps = memberConfig.getNetworkMBps() * 1000;
        NodeCapacity provisionedCapacity = new NodeCapacity(memberConfig.getMaxQps(), networkKBps);
        return new MemberInfo(
            hostName,
            hostAddress,
            memberConfig.getClusterPort(),
            memberConfig.getRoles(),
            provisionedCapacity
        );
    }

    private static VaradhiZkClusterManager getClusterManager(AppConfiguration config, String host) {
        CuratorFramework curatorFramework = CuratorFrameworkCreator.create(config.getZookeeperOptions());
        DeliveryOptions deliveryOptions = new DeliveryOptions();
        deliveryOptions.setTracingPolicy(config.getDeliveryOptions().getTracingPolicy());
        deliveryOptions.setSendTimeout(config.getDeliveryOptions().getTimeoutMs());
        return new VaradhiZkClusterManager(curatorFramework, deliveryOptions, host);
    }

    private static Future<Vertx> createClusteredVertx(
        AppConfiguration config,
        ClusterManager clusterManager,
        CoreServices services,
        MemberInfo memberInfo
    ) {
        int port = 0;
        JsonObject memberInfoJson = new JsonObject(JsonMapper.jsonSerialize(memberInfo));
        EventBusOptions eventBusOptions = new EventBusOptions().setHost(memberInfo.hostname())
                                                               .setPort(port)
                                                               .setClusterPublicHost(memberInfo.address())
                                                               .setClusterNodeMetadata(memberInfoJson);

        var metricsOptions = new MicrometerMetricsOptions().setMicrometerRegistry(services.getMeterRegistry())
                                                           .setMetricsNaming(MetricsNaming.v4Names())
                                                           .setRegistryName("default")
                                                           .addDisabledMetricsCategory(MetricsDomain.HTTP_SERVER)
                                                           .setJvmMetricsEnabled(true)
                                                           .setEnabled(true);

        VertxOptions vertxOptions = config.getVertxOptions()
                                          .setTracingOptions(new OpenTelemetryOptions(services.getOpenTelemetry()))
                                          .setMetricsOptions(metricsOptions)
                                          .setEventBusOptions(eventBusOptions);

        return Vertx.builder().with(vertxOptions).withClusterManager(clusterManager).buildClustered();
    }

    public static Pair<AppConfiguration, ConfigFileResolver> readConfiguration(String[] args) {
        if (args.length < 1) {
            log.error("Usage: java com.flipkart.varadhi.VaradhiApplication configuration.yml");
            System.exit(-1);
        }
        String mainConfigPath = args[0];
        ConfigFileResolver configResolver = nameOrPath -> Paths.get(args[0]).resolveSibling(nameOrPath).toString();
        return Pair.of(resolveLinkedConfigFiles(configResolver, readConfigFromFile(mainConfigPath)), configResolver);
    }

    public static AppConfiguration readConfigFromFile(String filePath) throws InvalidConfigException {
        log.info("Loading Configuration.");
        Vertx vertx = Vertx.vertx();

        ConfigStoreOptions fileStore = new ConfigStoreOptions().setType("file")
                                                               .setOptional(false)
                                                               .setFormat("yaml")
                                                               .setConfig(new JsonObject().put("path", filePath));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        try {
            JsonObject content = retriever.getConfig().toCompletionStage().toCompletableFuture().join();
            AppConfiguration configuration = content.mapTo(AppConfiguration.class);
            configuration.validate();
            return configuration;
        } catch (Exception e) {
            throw new InvalidConfigException("Failed to load Application Configuration", e);
        } finally {
            retriever.close();
            vertx.close();
        }
    }

    public static AppConfiguration resolveLinkedConfigFiles(
        ConfigFileResolver configResolver,
        AppConfiguration config
    ) {
        RecursiveFieldUpdater.visit(config, ConfigFile.class, (field, value) -> {
            if (value instanceof String path) {
                if (path.endsWith(".yml")) {
                    // read file and update the field
                    String resolvedPath = configResolver.resolve(path);
                    if (!resolvedPath.equals(path)) {
                        log.info("Resolved the config file at {} to {}", field, resolvedPath);
                    }
                    return resolvedPath;
                }
                throw new InvalidConfigException("config : " + field + " is not a yml file path");
            } else {
                throw new InvalidConfigException("config : " + field + " is not a string.");
            }
        });
        return config;
    }

    private static Map<ComponentKind, Verticle> getComponentVerticles(
        AppConfiguration config,
        CoreServices coreServices,
        VaradhiClusterManager clusterManager,
        MemberInfo memberInfo
    ) {
        return Arrays.stream(memberInfo.roles())
                     .distinct()
                     .collect(Collectors.toMap(Function.identity(), kind -> switch (kind) {
                         case Server -> new WebServerVerticle(config, coreServices, clusterManager);
                         case Controller -> new ControllerVerticle(
                             config.getController(),
                             coreServices,
                             clusterManager
                         );
                         case Consumer -> new ConsumerVerticle(coreServices, memberInfo, clusterManager);
                     }));
    }
}
