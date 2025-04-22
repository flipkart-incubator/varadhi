package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.auth.DefaultAuthorizationProvider;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.common.EntityReadCacheRegistry;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.core.cluster.ControllerRestApi;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.DlqService;
import com.flipkart.varadhi.services.IamPolicyService;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.services.TeamService;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.spi.db.IamPolicyStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.utils.ShardProvisioner;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.utils.VaradhiTopicFactory;
import com.flipkart.varadhi.verticles.consumer.ConsumerClientFactoryImpl;
import com.flipkart.varadhi.verticles.controller.ControllerRestClient;
import com.flipkart.varadhi.web.AuthnHandler;
import com.flipkart.varadhi.web.AuthzHandler;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.HierarchyHandler;
import com.flipkart.varadhi.web.RequestBodyHandler;
import com.flipkart.varadhi.web.RequestBodyParser;
import com.flipkart.varadhi.web.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.HealthCheckHandler;
import com.flipkart.varadhi.web.v1.admin.DlqHandlers;
import com.flipkart.varadhi.web.v1.admin.OrgFilterHandler;
import com.flipkart.varadhi.web.v1.admin.OrgHandlers;
import com.flipkart.varadhi.web.v1.admin.ProjectHandlers;
import com.flipkart.varadhi.web.v1.admin.SubscriptionHandlers;
import com.flipkart.varadhi.web.v1.admin.TeamHandlers;
import com.flipkart.varadhi.web.v1.admin.TopicHandlers;
import com.flipkart.varadhi.web.v1.authz.IamPolicyHandlers;
import com.flipkart.varadhi.web.v1.produce.PreProduceHandler;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@ExtensionMethod ({Extensions.RoutingContextExtension.class})
public class WebServerVerticle extends AbstractVerticle {

    /**
     * Holds configuration data for the WebServer verticle
     */
    private record VerticleConfig(
        String deployedRegion,
        TopicCapacityPolicy defaultTopicCapacity,
        boolean isLeanDeployment
    ) {
        /**
         * Creates a VerticleConfig from the application configuration.
         *
         * @param configuration the application configuration
         * @return a new VerticleConfig instance
         */
        static VerticleConfig fromConfig(AppConfiguration configuration) {
            return new VerticleConfig(
                configuration.getRestOptions().getDeployedRegion(),
                configuration.getRestOptions().getDefaultTopicCapacity(),
                configuration.getFeatureFlags().isLeanDeployment()
            );
        }
    }

    // Immutable configuration and core services
    private final Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfigurators = new ConcurrentHashMap<>();
    private final AppConfiguration configuration;
    private final ConfigFileResolver configResolver;
    private final VaradhiClusterManager clusterManager;
    @SuppressWarnings ("rawtypes")
    private final MessagingStackProvider messagingStackProvider;
    private final MetaStore metaStore;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final VerticleConfig verticleConfig;
    private final EntityReadCacheRegistry cacheRegistry;
    private final List<Pattern> disableAPIPatterns;

    // Services initialized during startup
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();
    private HttpServer httpServer;

    /**
     * Creates a new WebServerVerticle with the specified configuration and services.
     *
     * @param configuration  the application configuration
     * @param services       the core services
     * @param clusterManager the cluster manager
     */
    public WebServerVerticle(
        AppConfiguration configuration,
        CoreServices services,
        VaradhiClusterManager clusterManager,
        EntityReadCacheRegistry cacheRegistry
    ) {
        this.configuration = configuration;
        this.configResolver = services.getConfigResolver();
        this.clusterManager = clusterManager;
        this.messagingStackProvider = services.getMessagingStackProvider();
        this.metaStore = services.getMetaStoreProvider().getMetaStore();
        this.meterRegistry = services.getMeterRegistry();
        this.tracer = services.getTracer("varadhi");
        this.verticleConfig = VerticleConfig.fromConfig(configuration);
        this.cacheRegistry = cacheRegistry;
        this.disableAPIPatterns = configuration.getDisabledAPIs()
                .stream()
                .map(Pattern::compile)
                .collect(Collectors.toList());
    }

    /**
     * Wraps a handler in a blocking execution context using virtual threads.
     * This allows handlers to perform blocking operations without affecting the event loop.
     *
     * @param vertx         the Vert.x instance
     * @param apiEndHandler the handler to wrap
     * @return a handler that executes the original handler in a virtual thread
     */
    public static Handler<RoutingContext> wrapBlockingExecution(Vertx vertx, Handler<RoutingContext> apiEndHandler) {
        return ctx -> {
            Future<Void> future = vertx.executeBlocking(() -> {
                apiEndHandler.handle(ctx);
                return null;
            });
            future.onComplete(resultHandler -> {
                if (resultHandler.succeeded()) {
                    if (ctx.getApiResponse() == null) {
                        ctx.endRequest();
                    } else {
                        ctx.endRequestWithResponse(ctx.getApiResponse());
                    }
                } else {
                    ctx.endRequestWithException(resultHandler.cause());
                }
            });
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(Promise<Void> startPromise) {
        log.info("Starting WebServer verticle");

        setupEntityServices().compose(v -> {
            performValidations();
            return startHttpServer();
        }).onSuccess(v -> {
            log.info("WebServer verticle started successfully");
            startPromise.complete();
        }).onFailure(e -> {
            log.error("Failed to start WebServer verticle", e);
            startPromise.fail(e);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("Stopping HttpServer");
        if (httpServer != null) {
            httpServer.close(stopPromise);
        } else {
            stopPromise.complete();
        }
    }

    /**
     * Initializes all entity services required by the web server.
     * Uses a builder pattern for cleaner initialization.
     *
     * @return a Future that completes when all services are initialized
     */
    @SuppressWarnings ("unchecked")
    private Future<Void> setupEntityServices() {
        log.info("Setting up entity services");

        // Create message exchange for communication
        MessageExchange messageExchange = clusterManager.getExchange(vertx);

        // Initialize basic services
        serviceRegistry.register(OrgService.class, new OrgService(metaStore.orgs(), metaStore.teams()));
        serviceRegistry.register(TeamService.class, new TeamService(metaStore));
        serviceRegistry.register(ProjectService.class, new ProjectService(metaStore));

        // Initialize topic service
        serviceRegistry.register(
            VaradhiTopicService.class,
            new VaradhiTopicService(
                messagingStackProvider.getStorageTopicService(),
                metaStore.topics(),
                metaStore.subscriptions(),
                metaStore.projects()
            )
        );

        // Initialize controller client and related services
        ControllerRestApi controllerClient = new ControllerRestClient(messageExchange);
        ShardProvisioner shardProvisioner = new ShardProvisioner(
            messagingStackProvider.getStorageSubscriptionService(),
            messagingStackProvider.getStorageTopicService()
        );

        // Initialize subscription and DLQ services
        serviceRegistry.register(
            SubscriptionService.class,
            new SubscriptionService(shardProvisioner, controllerClient, metaStore.subscriptions(), metaStore.topics())
        );
        serviceRegistry.register(
            DlqService.class,
            new DlqService(controllerClient, new ConsumerClientFactoryImpl(messageExchange))
        );

        // Initialize producer service
        Function<StorageTopic, Producer> producerProvider = messagingStackProvider.getProducerFactory()::newProducer;

        serviceRegistry.register(
            ProducerService.class,
            new ProducerService(
                verticleConfig.deployedRegion(),
                producerProvider,
                cacheRegistry.getCache(ResourceType.TOPIC)
            )
        );

        return Future.succeededFuture();
    }

    /**
     * Performs validation checks based on the deployment configuration.
     */
    private void performValidations() {
        if (verticleConfig.isLeanDeployment()) {
            log.info("Performing lean deployment validations");
            // Its sync execution for time being, can be changed to Async.
            LeanDeploymentValidator validator = new LeanDeploymentValidator(
                serviceRegistry.get(OrgService.class),
                serviceRegistry.get(TeamService.class),
                serviceRegistry.get(ProjectService.class)
            );
            validator.validate(configuration.getRestOptions());
        }
    }

    /**
     * Starts the HTTP server with the configured routes.
     *
     * @return a Future that completes when the server is started
     */
    private Future<Void> startHttpServer() {
        Router router = createApiRouter();
        httpServer = vertx.createHttpServer(configuration.getHttpServerOptions()).requestHandler(router);

        return httpServer.listen()
                         .onSuccess(server -> log.info("HttpServer started on port {}", server.actualPort()))
                         .onFailure(cause -> log.error("HttpServer start failed: {}", cause.getMessage()))
                         .mapEmpty();
    }

    /**
     * Creates the API router with all configured routes.
     *
     * @return the configured Router instance
     */
    private Router createApiRouter() {
        Router router = Router.router(vertx);
        setupRouteConfigurators();

        // Collect all route definitions
        List<RouteDefinition> routeDefinitions = new ArrayList<>();
        routeDefinitions.addAll(getIamPolicyRoutes());
        routeDefinitions.addAll(getAdminApiRoutes());
        routeDefinitions.addAll(getProduceApiRoutes());

        routeDefinitions = routeDefinitions.stream().filter(this::isRouteEnabled).toList();

        // Configure all routes
        configureApiRoutes(router, routeDefinitions);
        return router;
    }

    /**
     * Determines if a route should be enabled based on configured disable patterns.
     *
     * @param routeDefinition the route definition to check
     * @return true if the route should be enabled, false otherwise
     */
    private boolean isRouteEnabled(RouteDefinition routeDefinition) {
        return disableAPIPatterns.stream()
                .noneMatch(pattern -> pattern.matcher(routeDefinition.getName()).matches());
    }

    /**
     * Gets IAM policy routes based on the configured authorization provider.
     *
     * @return a list of IAM policy route definitions
     */
    private List<RouteDefinition> getIamPolicyRoutes() {
        List<RouteDefinition> routes = new ArrayList<>();
        String authProviderName = configuration.getAuthorization() == null ?
            null :
            configuration.getAuthorization().getProviderClassName();
        boolean isDefaultProvider = DefaultAuthorizationProvider.class.getName().equals(authProviderName);

        if (isDefaultProvider) {
            if (metaStore instanceof IamPolicyStore.Provider iamPolicyMetaStore) {
                routes.addAll(
                    new IamPolicyHandlers(
                        serviceRegistry.get(ProjectService.class),
                        new IamPolicyService(metaStore, iamPolicyMetaStore.iamPolicies())
                    ).get()
                );
            } else {
                log.error(
                    "Incorrect Metastore for DefaultAuthorizationProvider. Expected IamPolicyMetaStore, found {}",
                    metaStore.getClass().getName()
                );
            }
        } else {
            log.info("Builtin IamPolicyRoutes are ignored, as {} is used as AuthorizationProvider", authProviderName);
        }
        return routes;
    }

    /**
     * Gets all admin API routes.
     *
     * @return a list of admin API route definitions
     */
    @SuppressWarnings ("unchecked")
    private List<RouteDefinition> getAdminApiRoutes() {
        List<RouteDefinition> routes = new ArrayList<>();

        // Create factories
        VaradhiTopicFactory varadhiTopicFactory = new VaradhiTopicFactory(
            messagingStackProvider.getStorageTopicFactory(),
            verticleConfig.deployedRegion(),
            verticleConfig.defaultTopicCapacity()
        );

        VaradhiSubscriptionFactory subscriptionFactory = new VaradhiSubscriptionFactory(
            messagingStackProvider.getStorageTopicService(),
            messagingStackProvider.getSubscriptionFactory(),
            messagingStackProvider.getStorageTopicFactory(),
            verticleConfig.deployedRegion()
        );

        // Add management entity routes if not in lean deployment
        routes.addAll(getManagementEntitiesApiRoutes());

        // Add topic, subscription, DLQ and health check routes
        routes.addAll(
            new TopicHandlers(
                varadhiTopicFactory,
                serviceRegistry.get(VaradhiTopicService.class),
                cacheRegistry.getCache(ResourceType.PROJECT)
            ).get()
        );

        routes.addAll(
            new SubscriptionHandlers(
                serviceRegistry.get(SubscriptionService.class),
                serviceRegistry.get(VaradhiTopicService.class),
                subscriptionFactory,
                configuration.getRestOptions(),
                cacheRegistry.getCache(ResourceType.PROJECT)
            ).get()
        );

        routes.addAll(
            new DlqHandlers(
                serviceRegistry.get(DlqService.class),
                serviceRegistry.get(SubscriptionService.class),
                cacheRegistry.getCache(ResourceType.PROJECT)
            ).get()
        );

        routes.addAll(new HealthCheckHandler().get());

        return routes;
    }

    /**
     * Gets management entity API routes based on deployment configuration.
     *
     * @return a list of management entity route definitions
     */
    private List<RouteDefinition> getManagementEntitiesApiRoutes() {
        List<RouteDefinition> routes = new ArrayList<>();
        if (!verticleConfig.isLeanDeployment()) {
            routes.addAll(new OrgHandlers(serviceRegistry.get(OrgService.class)).get());
            routes.addAll(new TeamHandlers(serviceRegistry.get(TeamService.class)).get());
            routes.addAll(
                new ProjectHandlers(
                    serviceRegistry.get(ProjectService.class),
                    cacheRegistry.getCache(ResourceType.PROJECT)
                ).get()
            );
            routes.addAll(new OrgFilterHandler(serviceRegistry.get(OrgService.class)).get());
        }
        return routes;
    }

    /**
     * Gets produce API routes.
     *
     * @return a list of produce API route definitions
     */
    private List<RouteDefinition> getProduceApiRoutes() {
        PreProduceHandler preProduceHandler = new PreProduceHandler();
        ProducerMetricHandler producerMetricsHandler = new ProducerMetricHandler(
            configuration.getProducerOptions().isMetricEnabled(),
            meterRegistry
        );

        return new ArrayList<>(
            new ProduceHandlers(
                serviceRegistry.get(ProducerService.class),
                preProduceHandler,
                producerMetricsHandler,
                configuration.getMessageConfiguration(),
                verticleConfig.deployedRegion(),
                cacheRegistry.getCache(ResourceType.PROJECT)
            ).get()
        );
    }

    /**
     * Sets up route configurators for different route behaviors.
     */
    private void setupRouteConfigurators() {
        AuthnHandler authnHandler = new AuthnHandler(vertx, configuration, meterRegistry);
        AuthzHandler authzHandler = new AuthzHandler(configuration, configResolver);
        RequestTelemetryConfigurator requestTelemetryConfigurator = new RequestTelemetryConfigurator(
            new SpanProvider(tracer),
            meterRegistry
        );

        // payload size restriction is required for Produce APIs. But should be fine to set as default for all.
        RequestBodyHandler requestBodyHandler = new RequestBodyHandler(
            configuration.getRestOptions().getPayloadSizeMax()
        );
        RequestBodyParser bodyParser = new RequestBodyParser();
        HierarchyHandler hierarchyHandler = new HierarchyHandler();

        // Register all route configurators
        routeBehaviourConfigurators.put(RouteBehaviour.telemetry, requestTelemetryConfigurator);
        routeBehaviourConfigurators.put(RouteBehaviour.authenticated, authnHandler);
        routeBehaviourConfigurators.put(RouteBehaviour.hasBody, (route, routeDef) -> route.handler(requestBodyHandler));
        routeBehaviourConfigurators.put(RouteBehaviour.parseBody, bodyParser);
        routeBehaviourConfigurators.put(RouteBehaviour.addHierarchy, hierarchyHandler);
        routeBehaviourConfigurators.put(RouteBehaviour.authorized, authzHandler);
    }

    /**
     * Configures API routes with the appropriate handlers and behaviors.
     *
     * @param router    the router to configure
     * @param apiRoutes the list of route definitions to configure
     */
    private void configureApiRoutes(Router router, List<RouteDefinition> apiRoutes) {
        log.info("Configuring {} API routes", apiRoutes.size());
        FailureHandler defaultFailureHandler = new FailureHandler();

        for (RouteDefinition def : apiRoutes) {
            Route route = router.route().method(def.getMethod()).path(def.getPath());

            // Sort behaviors by order and apply them
            RouteBehaviour[] behaviours = def.getBehaviours().toArray(new RouteBehaviour[0]);
            Arrays.sort(behaviours, Comparator.comparingInt(RouteBehaviour::getOrder));

            for (RouteBehaviour behaviour : behaviours) {
                RouteConfigurator configurator = routeBehaviourConfigurators.get(behaviour);
                if (configurator != null) {
                    configurator.configure(route, def);
                } else {
                    String errMsg = String.format("No RouteBehaviourProvider configured for %s.", behaviour);
                    log.error(errMsg);
                    throw new IllegalStateException(errMsg);
                }
            }

            // Add pre-handlers
            def.getPreHandlers().forEach(route::handler);

            // Add main handler (blocking or non-blocking)
            if (def.isBlockingEndHandler()) {
                route.handler(wrapBlockingExecution(vertx, def.getEndReqHandler()));
            } else {
                route.handler(def.getEndReqHandler());
            }

            // Add failure handler
            route.failureHandler(defaultFailureHandler);
        }
    }

    /**
     * Service registry for managing service instances.
     * Provides type-safe access to services.
     */
    private static class ServiceRegistry {
        private final Map<Class<?>, Object> services = new HashMap<>();

        /**
         * Registers a service instance with its class type.
         *
         * @param <T>     the service type
         * @param clazz   the service class
         * @param service the service instance
         */
        <T> void register(Class<T> clazz, T service) {
            services.put(clazz, Objects.requireNonNull(service, "Service cannot be null"));
        }

        /**
         * Gets a service instance by its class type.
         *
         * @param <T>   the service type
         * @param clazz the service class
         * @return the service instance
         * @throws IllegalStateException if the service is not registered
         */
        @SuppressWarnings ("unchecked")
        <T> T get(Class<T> clazz) {
            Object service = services.get(clazz);
            if (service == null) {
                throw new IllegalStateException("Service not registered: " + clazz.getName());
            }
            return (T)service;
        }
    }
}
