package com.flipkart.varadhi.verticles.webserver;

import java.util.*;
import java.util.function.Function;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.auth.DefaultAuthorizationProvider;
import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.core.cluster.ControllerRestApi;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.*;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.utils.ShardProvisioner;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.utils.VaradhiTopicFactory;
import com.flipkart.varadhi.verticles.consumer.ConsumerClientFactoryImpl;
import com.flipkart.varadhi.verticles.controller.ControllerRestClient;
import com.flipkart.varadhi.web.*;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.HealthCheckHandler;
import com.flipkart.varadhi.web.v1.admin.*;
import com.flipkart.varadhi.web.v1.authz.IamPolicyHandlers;
import com.flipkart.varadhi.web.v1.produce.PreProduceHandler;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@ExtensionMethod ({Extensions.RoutingContextExtension.class})
public class WebServerVerticle extends AbstractVerticle {
    private final Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfigurators = new HashMap<>();
    private final AppConfiguration configuration;
    private final ConfigFileResolver configResolver;
    private final VaradhiClusterManager clusterManager;
    private final MessagingStackProvider messagingStackProvider;
    private final MetaStore metaStore;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private OrgService orgService;
    private TeamService teamService;
    private ProjectService projectService;
    private VaradhiTopicService varadhiTopicService;
    private SubscriptionService subscriptionService;
    private DlqService dlqService;
    private HttpServer httpServer;

    public WebServerVerticle(
        AppConfiguration configuration,
        CoreServices services,
        VaradhiClusterManager clusterManager
    ) {
        this.configuration = configuration;
        this.configResolver = services.getConfigResolver();
        this.clusterManager = clusterManager;
        this.messagingStackProvider = services.getMessagingStackProvider();
        this.metaStore = services.getMetaStoreProvider().getMetaStore();
        this.meterRegistry = services.getMeterRegistry();
        this.tracer = services.getTracer("varadhi");
    }

    public static Handler<RoutingContext> wrapBlockingExecution(Vertx vertx, Handler<RoutingContext> apiEndHandler) {
        // no try/catch around apiEndHandler.handle as executeBlocking does the same and fails the future.
        return ctx -> {
            Future<Void> future = vertx.executeBlocking(() -> {
                apiEndHandler.handle(ctx);
                return null;
            });
            future.onComplete(resultHandler -> {
                if (resultHandler.succeeded()) {
                    if (null == ctx.getApiResponse()) {
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

    @Override
    public void start(Promise<Void> startPromise) {
        setupEntityServices();
        performValidations();
        startHttpServer(startPromise);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("HttpServer Stopping.");
        httpServer.close(h -> {
            log.info("HttpServer Stopped.");
            stopPromise.complete();
        });
    }

    private void setupEntityServices() {
        String projectCacheSpec = configuration.getRestOptions().getProjectCacheBuilderSpec();
        orgService = new OrgService(metaStore.orgMetaStore(), metaStore.teamMetaStore());
        teamService = new TeamService(metaStore);
        projectService = new ProjectService(metaStore, projectCacheSpec, meterRegistry);
        varadhiTopicService = new VaradhiTopicService(
            messagingStackProvider.getStorageTopicService(),
            metaStore.topicMetaStore(),
            metaStore.subscriptionMetaStore(),
            metaStore.projectMetaStore()
        );
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        ControllerRestApi controllerClient = new ControllerRestClient(messageExchange);
        ShardProvisioner shardProvisioner = new ShardProvisioner(
            messagingStackProvider.getStorageSubscriptionService(),
            messagingStackProvider.getStorageTopicService()
        );
        subscriptionService = new SubscriptionService(
            shardProvisioner,
            controllerClient,
            metaStore.subscriptionMetaStore(),
            metaStore.topicMetaStore()
        );
        dlqService = new DlqService(controllerClient, new ConsumerClientFactoryImpl(messageExchange));

    }

    private void performValidations() {
        if (configuration.getFeatureFlags().isLeanDeployment()) {
            // Its sync execution for time being, can be changed to Async.
            LeanDeploymentValidator validator = new LeanDeploymentValidator(orgService, teamService, projectService);
            validator.validate(configuration.getRestOptions());
        }
    }

    private void startHttpServer(Promise<Void> startPromise) {
        Router router = createApiRouter();
        httpServer = vertx.createHttpServer(configuration.getHttpServerOptions()).requestHandler(router).listen(h -> {
            if (h.succeeded()) {
                log.info("HttpServer Started.");
                startPromise.complete();
            } else {
                log.warn("HttpServer Start Failed." + h.cause());
                startPromise.fail("HttpServer Start Failed." + h.cause());
            }
        });
    }

    private Router createApiRouter() {
        Router router = Router.router(vertx);
        List<RouteDefinition> routeDefinitions = new ArrayList<>();
        setupRouteConfigurators();
        routeDefinitions.addAll(getIamPolicyRoutes());
        routeDefinitions.addAll(getAdminApiRoutes());
        routeDefinitions.addAll(getProduceApiRoutes());
        configureApiRoutes(router, routeDefinitions);
        return router;
    }

    private List<RouteDefinition> getIamPolicyRoutes() {
        List<RouteDefinition> routes = new ArrayList<>();
        String authProviderName = configuration.getAuthorization() == null ?
            null :
            configuration.getAuthorization().getProviderClassName();
        boolean isDefaultProvider = DefaultAuthorizationProvider.class.getName().equals(authProviderName);
        boolean isIamPolicyStore = metaStore.iamPolicyMetaStore() == null;
        //TODO::Validate below specifically w.r.to lean deployment.
        // enable IamPolicy Routes, if
        // 1. provider class name is DefaultAuthorizationProvider, and
        // 2. metastore is RoleBindingMetastore
        // This is independent of Authorization is enabled or not
        if (isDefaultProvider) {
            if (isIamPolicyStore) {
                routes.addAll(
                    new IamPolicyHandlers(
                        projectService,
                        new IamPolicyService(metaStore, metaStore.iamPolicyMetaStore())
                    ).get()
                );
            } else {
                log.error(
                    "Incorrect Metastore for DefaultAuthorizationProvider. Expected RoleBindingMetaStore, found {}",
                    metaStore.getClass().getName()
                );
            }
        } else {
            log.info("Builtin IamPolicyRoutes are ignored, as {} is used as AuthorizationProvider", authProviderName);
        }
        return routes;
    }

    @SuppressWarnings ("unchecked")
    private List<RouteDefinition> getAdminApiRoutes() {
        List<RouteDefinition> routes = new ArrayList<>();
        TopicCapacityPolicy defaultTopicCapacity = configuration.getRestOptions().getDefaultTopicCapacity();
        String deployedRegion = configuration.getRestOptions().getDeployedRegion();
        VaradhiTopicFactory varadhiTopicFactory = new VaradhiTopicFactory(
            messagingStackProvider.getStorageTopicFactory(),
            deployedRegion,
            defaultTopicCapacity
        );
        VaradhiSubscriptionFactory subscriptionFactory = new VaradhiSubscriptionFactory(
            messagingStackProvider.getStorageTopicService(),
            messagingStackProvider.getSubscriptionFactory(),
            messagingStackProvider.getStorageTopicFactory(),
            deployedRegion
        );
        routes.addAll(getManagementEntitiesApiRoutes());
        routes.addAll(new TopicHandlers(varadhiTopicFactory, varadhiTopicService, projectService).get());
        routes.addAll(
            new SubscriptionHandlers(
                subscriptionService,
                projectService,
                varadhiTopicService,
                subscriptionFactory,
                configuration.getRestOptions()
            ).get()
        );
        routes.addAll(new DlqHandlers(dlqService, subscriptionService, projectService).get());
        routes.addAll(new HealthCheckHandler().get());
        return routes;
    }

    private List<RouteDefinition> getManagementEntitiesApiRoutes() {
        List<RouteDefinition> routes = new ArrayList<>();
        if (!configuration.getFeatureFlags().isLeanDeployment()) {
            routes.addAll(new OrgHandlers(orgService).get());
            routes.addAll(new TeamHandlers(teamService).get());
            routes.addAll(new ProjectHandlers(projectService).get());
            routes.addAll(new OrgFilterHandler(orgService).get());
        }
        return routes;
    }

    @SuppressWarnings ("unchecked")
    private List<RouteDefinition> getProduceApiRoutes() {
        String deployedRegion = configuration.getRestOptions().getDeployedRegion();
        Function<String, VaradhiTopic> topicProvider = varadhiTopicService::get;
        PreProduceHandler preProduceHandler = new PreProduceHandler();
        Function<StorageTopic, Producer> producerProvider = messagingStackProvider.getProducerFactory()::newProducer;

        ProducerService producerService = new ProducerService(
            deployedRegion,
            configuration.getProducerOptions(),
            producerProvider,
            topicProvider,
            meterRegistry
        );
        ProducerMetricHandler producerMetricsHandler = new ProducerMetricHandler(
            configuration.getProducerOptions().isMetricEnabled(),
            meterRegistry
        );
        return new ArrayList<>(
            new ProduceHandlers(
                producerService,
                preProduceHandler,
                projectService,
                varadhiTopicService,
                orgService,
                producerMetricsHandler,
                configuration.getMessageConfiguration(),
                deployedRegion
            ).get()
        );
    }

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
        routeBehaviourConfigurators.put(RouteBehaviour.telemetry, requestTelemetryConfigurator);
        routeBehaviourConfigurators.put(RouteBehaviour.authenticated, authnHandler);
        routeBehaviourConfigurators.put(RouteBehaviour.hasBody, (route, routeDef) -> route.handler(requestBodyHandler));
        routeBehaviourConfigurators.put(RouteBehaviour.parseBody, bodyParser);
        routeBehaviourConfigurators.put(RouteBehaviour.addHierarchy, hierarchyHandler);
        routeBehaviourConfigurators.put(RouteBehaviour.authorized, authzHandler);
    }

    private void configureApiRoutes(Router router, List<RouteDefinition> apiRoutes) {
        log.info("Configuring API routes.");
        FailureHandler defaultFailureHandler = new FailureHandler();
        for (RouteDefinition def : apiRoutes) {
            Route route = router.route().method(def.getMethod()).path(def.getPath());
            RouteBehaviour[] behaviours = def.getBehaviours().toArray(new RouteBehaviour[0]);
            Arrays.sort(behaviours, Comparator.comparingInt(RouteBehaviour::getOrder));
            for (RouteBehaviour behaviour : behaviours) {
                RouteConfigurator routeConfigurator = routeBehaviourConfigurators.getOrDefault(behaviour, null);
                if (null != routeConfigurator) {
                    routeConfigurator.configure(route, def);
                } else {
                    String errMsg = String.format("No RouteBehaviourProvider configured for %s.", behaviour);
                    log.error(errMsg);
                    throw new IllegalStateException(errMsg);
                }
            }
            def.getPreHandlers().forEach(route::handler);
            if (def.isBlockingEndHandler()) {
                route.handler(wrapBlockingExecution(vertx, def.getEndReqHandler()));
            } else {
                route.handler(def.getEndReqHandler());
            }
            route.failureHandler(defaultFailureHandler);
        }
    }
}
