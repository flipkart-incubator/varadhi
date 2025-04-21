package com.flipkart.varadhi;

import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.custom.VaradhiZkClusterManager;
import com.flipkart.varadhi.common.EntityReadCache;
import com.flipkart.varadhi.common.EntityReadCacheRegistry;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.common.reflect.RecursiveFieldUpdater;
import com.flipkart.varadhi.common.utils.HostUtils;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.MemberConfig;
import com.flipkart.varadhi.core.cluster.entities.ComponentKind;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.core.cluster.entities.NodeCapacity;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.events.EntityEventManager;
import com.flipkart.varadhi.spi.ConfigFile;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import com.flipkart.varadhi.verticles.consumer.ConsumerVerticle;
import com.flipkart.varadhi.verticles.controller.ControllerVerticle;
import com.flipkart.varadhi.verticles.webserver.WebServerVerticle;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
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
import io.vertx.micrometer.MicrometerMetricsFactory;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryTracingFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;

import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Main application class for Varadhi, responsible for bootstrapping the system.
 * <p>
 * This class follows a structured initialization process:
 * <ol>
 *   <li>Load and validate configuration</li>
 *   <li>Initialize core services and utilities</li>
 *   <li>Set up the cluster manager</li>
 *   <li>Initialize entity caches and event handlers</li>
 *   <li>Deploy verticles for different components</li>
 * </ol>
 */
@Slf4j
public class VaradhiApplication {

    /**
     * Application entry point.
     * <p>
     * Initializes and starts the Varadhi application with the provided configuration.
     * Uses a structured, reactive approach with proper error handling.
     *
     * @param args command-line arguments, where the first argument must be the path to the configuration file
     */
    public static void main(String[] args) throws UnknownHostException {
        // Read configuration
        Pair<AppConfiguration, ConfigFileResolver> configReadResult = readConfiguration(args);
        AppConfiguration configuration = configReadResult.getLeft();
        ConfigFileResolver configResolver = configReadResult.getRight();

        // Initialize host utilities and standard headers
        HostUtils.init();
        StdHeaders.init(configuration.getMessageConfiguration().getStdHeaders());

        // Set up member info and core services
        MemberInfo memberInfo = getMemberInfo(configuration.getMember());
        CoreServices services = new CoreServices(configuration, configResolver);
        VaradhiZkClusterManager clusterManager = getClusterManager(configuration, memberInfo.hostname());

        Future<Pair<Vertx, Map<ComponentKind, Verticle>>> initFuture = createClusteredVertx(
            configuration,
            clusterManager,
            services,
            memberInfo
        ).compose(vertx -> initializeEventManager(services, clusterManager, memberInfo, vertx).map(cacheRegistry -> {
            log.info("Caches and event handlers initialized successfully");

            // Get component verticles
            Map<ComponentKind, Verticle> verticles = getComponentVerticles(
                configuration,
                services,
                clusterManager,
                memberInfo,
                cacheRegistry
            );

            // Return both for the next step
            return Pair.of(vertx, verticles);
        }));

        // Deploy verticles and handle success/failure
        initFuture.compose(pair -> deployVerticles(pair.getLeft(), pair.getRight()))
                  .onSuccess(v -> log.info("Started successfully on {}", memberInfo.hostname()))
                  .onFailure(t -> {
                      log.error("Failed to start: {}", t.getMessage());
                      System.exit(-1);
                  });

        // TODO: Check need for shutdown hook
    }

    /**
     * Creates member information from configuration.
     *
     * @param memberConfig the member configuration
     * @return member information object
     */
    private static MemberInfo getMemberInfo(MemberConfig memberConfig) {
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

    /**
     * Creates and initializes the cluster manager.
     *
     * @param config the application configuration
     * @param host   the hostname
     * @return initialized cluster manager
     */
    private static VaradhiZkClusterManager getClusterManager(AppConfiguration config, String host) {
        CuratorFramework curatorFramework = CuratorFrameworkCreator.create(config.getZookeeperOptions());
        DeliveryOptions deliveryOptions = new DeliveryOptions().setTracingPolicy(
            config.getDeliveryOptions().getTracingPolicy()
        ).setSendTimeout(config.getDeliveryOptions().getTimeoutMs());

        return new VaradhiZkClusterManager(curatorFramework, deliveryOptions, host);
    }

    /**
     * Initializes the EventManager to set up caches and event handlers.
     *
     * @param services       core services
     * @param clusterManager cluster manager
     * @param memberInfo     member information
     * @param vertx          the Vert.x instance to use for event handling
     * @return a future that completes with the initialized EntityReadCacheRegistry
     */
    private static Future<EntityReadCacheRegistry> initializeEventManager(
        CoreServices services,
        VaradhiClusterManager clusterManager,
        MemberInfo memberInfo,
        Vertx vertx
    ) {
        // Create registry and prepare cache creation futures
        MetaStore metaStore = services.getMetaStoreProvider().getMetaStore();
        EntityReadCacheRegistry registry = new EntityReadCacheRegistry();

        // Create futures for cache creation and preloading
        Future<EntityReadCache<Project>> projectCacheFuture = EntityReadCache.create(
            ResourceType.PROJECT,
            metaStore.projects()::getAll
        );

        Future<EntityReadCache<VaradhiTopic>> topicCacheFuture = EntityReadCache.create(
            ResourceType.TOPIC,
            metaStore.topics()::getAll
        );

        // Combine futures and register caches when they're ready
        return Future.all(projectCacheFuture, topicCacheFuture).map(v -> {
            // Register preloaded caches
            registry.register(ResourceType.PROJECT, projectCacheFuture.result());
            registry.register(ResourceType.TOPIC, topicCacheFuture.result());

            return registry;
        }).compose(reg -> {
            // Create and initialize entity event manager
            EntityEventManager entityEventManager = new EntityEventManager(reg, clusterManager, memberInfo, vertx);
            return entityEventManager.initialize().map(v -> reg);
        });
    }

    /**
     * Creates a clustered Vert.x instance with the specified configuration.
     *
     * @param config         application configuration
     * @param clusterManager cluster manager
     * @param services       core services
     * @param memberInfo     member information
     * @return a future that completes with the created Vert.x instance
     */
    private static Future<Vertx> createClusteredVertx(
        AppConfiguration config,
        ClusterManager clusterManager,
        CoreServices services,
        MemberInfo memberInfo
    ) {
        log.info("Creating clustered Vert.x instance");

        // Configure event bus options
        JsonObject memberInfoJson = new JsonObject(JsonMapper.jsonSerialize(memberInfo));
        EventBusOptions eventBusOptions = new EventBusOptions().setHost(memberInfo.hostname())
                                                               .setPort(0)
                                                               .setClusterPublicHost(memberInfo.address())
                                                               .setClusterNodeMetadata(memberInfoJson);

        // Configure metrics options
        MeterRegistry meterRegistry = services.getMeterRegistry();
        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions();
        metricsOptions.setFactory(new MicrometerMetricsFactory(meterRegistry))
                      .setMetricsNaming(MetricsNaming.v4Names())
                      .setRegistryName("default")
                      .addDisabledMetricsCategory(MetricsDomain.HTTP_SERVER)
                      .setJvmMetricsEnabled(true)
                      .setEnabled(true);

        // Configure Vert.x options
        VertxOptions vertxOptions = config.getVertxOptions()
                                          .setMetricsOptions(metricsOptions)
                                          .setEventBusOptions(eventBusOptions);

        // Get OpenTelemetry instance
        OpenTelemetry openTelemetry = services.getOpenTelemetry();

        // Build clustered Vert.x
        return Vertx.builder()
                    .with(vertxOptions)
                    .withClusterManager(clusterManager)
                    .withTracer(new OpenTelemetryTracingFactory(openTelemetry))
                    .buildClustered();
    }

    /**
     * Deploys verticles for all components.
     *
     * @param vertx     the Vert.x instance
     * @param verticles map of component kinds to verticle instances
     * @return a future that completes when all verticles are deployed
     */
    private static Future<Void> deployVerticles(Vertx vertx, Map<ComponentKind, Verticle> verticles) {
        return Future.all(verticles.entrySet().stream().map(entry -> {
            ComponentKind kind = entry.getKey();
            Verticle verticle = entry.getValue();

            return vertx.deployVerticle(verticle).onComplete(ar -> {
                if (!ar.succeeded()) {
                    log.error("Component '{}' failed to start: {}", kind, ar.cause().getMessage());
                }
            });
        }).toList()).mapEmpty();
    }

    /**
     * Reads and validates configuration from the specified file.
     *
     * @param args command-line arguments, where the first argument must be the path to the configuration file
     * @return a pair containing the application configuration and config file resolver
     */
    public static Pair<AppConfiguration, ConfigFileResolver> readConfiguration(String[] args) {
        if (args.length < 1) {
            log.error("Usage: java com.flipkart.varadhi.VaradhiApplication configuration.yml");
            System.exit(-1);
        }

        String mainConfigPath = args[0];
        ConfigFileResolver configResolver = nameOrPath -> Paths.get(args[0]).resolveSibling(nameOrPath).toString();

        return Pair.of(resolveLinkedConfigFiles(configResolver, readConfigFromFile(mainConfigPath)), configResolver);
    }

    /**
     * Reads configuration from a YAML file.
     *
     * @param filePath path to the configuration file
     * @return the application configuration
     * @throws InvalidConfigException if the configuration is invalid or cannot be read
     */
    public static AppConfiguration readConfigFromFile(String filePath) throws InvalidConfigException {
        log.info("Loading configuration from {}", filePath);
        Vertx vertx = Vertx.vertx();
        ConfigRetriever retriever = null;

        try {
            ConfigStoreOptions fileStore = new ConfigStoreOptions().setType("file")
                                                                   .setOptional(false)
                                                                   .setFormat("yaml")
                                                                   .setConfig(new JsonObject().put("path", filePath));

            ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);
            retriever = ConfigRetriever.create(vertx, options);

            JsonObject content = retriever.getConfig().toCompletionStage().toCompletableFuture().join();

            AppConfiguration configuration = content.mapTo(AppConfiguration.class);
            configuration.validate();
            return configuration;
        } catch (Exception e) {
            throw new InvalidConfigException("Failed to load Application Configuration", e);
        } finally {
            if (retriever != null) {
                retriever.close();
            }
            vertx.close();
        }
    }

    /**
     * Resolves linked configuration files referenced in the main configuration.
     *
     * @param configResolver resolver for configuration file paths
     * @param config         the application configuration
     * @return the updated application configuration with resolved linked files
     */
    public static AppConfiguration resolveLinkedConfigFiles(
        ConfigFileResolver configResolver,
        AppConfiguration config
    ) {
        RecursiveFieldUpdater.visit(config, ConfigFile.class, (field, value) -> {
            if (value instanceof String path) {
                if (path.endsWith(".yml")) {
                    // Read file and update the field
                    String resolvedPath = configResolver.resolve(path);
                    if (!resolvedPath.equals(path)) {
                        log.info("Resolved the config file at {} to {}", field, resolvedPath);
                    }
                    return resolvedPath;
                }
                throw new InvalidConfigException("Config '" + field + "' is not a yml file path");
            } else {
                throw new InvalidConfigException("Config '" + field + "' is not a string");
            }
        });

        return config;
    }

    /**
     * Creates verticles for all enabled components.
     *
     * @param config         application configuration
     * @param coreServices   core services
     * @param clusterManager cluster manager
     * @param memberInfo     member information
     * @return a map of component kinds to verticle instances
     */
    private static Map<ComponentKind, Verticle> getComponentVerticles(
        AppConfiguration config,
        CoreServices coreServices,
        VaradhiClusterManager clusterManager,
        MemberInfo memberInfo,
        EntityReadCacheRegistry cacheRegistry
    ) {
        log.info("Creating verticles for components: {}", Arrays.toString(memberInfo.roles()));

        return Arrays.stream(memberInfo.roles())
                     .distinct()
                     .collect(Collectors.toMap(Function.identity(), kind -> switch (kind) {
                         case Server -> new WebServerVerticle(config, coreServices, clusterManager, cacheRegistry);
                         case Controller -> new ControllerVerticle(
                             config.getController(),
                             coreServices,
                             clusterManager
                         );
                         case Consumer -> new ConsumerVerticle(coreServices, memberInfo, clusterManager);
                     }));
    }
}
