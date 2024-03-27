package com.flipkart.varadhi;

import com.flipkart.varadhi.auth.DefaultAuthorizationProvider;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.core.VaradhiTopicFactory;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.ophandlers.ControllerApi;
import com.flipkart.varadhi.core.proxies.ControllerApiProxy;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.*;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.RoleBindingMetaStore;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.web.*;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.HealthCheckHandler;
import com.flipkart.varadhi.web.v1.admin.SubscriptionHandlers;
import com.flipkart.varadhi.web.v1.admin.TopicHandlers;
import com.flipkart.varadhi.web.v1.authz.IamPolicyHandlers;
import com.flipkart.varadhi.web.v1.produce.HeaderValidationHandler;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
public abstract class VerticleDeployer {
    protected final OrgService orgService;
    protected final TeamService teamService;
    protected final ProjectService projectService;
    private final TopicHandlers topicHandlers;
    private final ProduceHandlers produceHandlers;
    private final SubscriptionHandlers subscriptionHandlers;
    private final HealthCheckHandler healthCheckHandler;
    private final Supplier<IamPolicyHandlers> authZHandlersSupplier;
    private final Map<RouteBehaviour, RouteConfigurator> behaviorConfigurators = new HashMap<>();

    public VerticleDeployer(
            Vertx vertx,
            AppConfiguration configuration,
            MessagingStackProvider messagingStackProvider,
            MetaStoreProvider metaStoreProvider,
            MessageChannel messageChannel,
            MeterRegistry meterRegistry,
            Tracer tracer
    ) {
        RestOptions restOptions = configuration.getRestOptions();
        String deployedRegion = restOptions.getDeployedRegion();
        VaradhiTopicFactory varadhiTopicFactory =
                new VaradhiTopicFactory(messagingStackProvider.getStorageTopicFactory(), deployedRegion);
        VaradhiTopicService varadhiTopicService = new VaradhiTopicService(
                messagingStackProvider.getStorageTopicService(),
                metaStoreProvider.getMetaStore()
        );
        MetaStore metaStore = metaStoreProvider.getMetaStore();
        this.projectService =
                new ProjectService(metaStore, restOptions.getProjectCacheBuilderSpec(), meterRegistry);
        this.topicHandlers =
                new TopicHandlers(varadhiTopicFactory, varadhiTopicService, projectService);
        ProducerService producerService = new ProducerService(deployedRegion, configuration.getProducerOptions(),
                messagingStackProvider.getProducerFactory(), varadhiTopicService, meterRegistry
        );
        this.orgService = new OrgService(metaStore);
        this.teamService = new TeamService(metaStore);
        HeaderValidationHandler headerValidator = new HeaderValidationHandler(restOptions);
        ProducerMetricHandler producerMetricsHandler = new ProducerMetricHandler(
                configuration.getProducerOptions().isMetricEnabled(), meterRegistry);

        this.produceHandlers =
                new ProduceHandlers(
                        deployedRegion, headerValidator::validate, producerService, projectService,
                        producerMetricsHandler
                );
        this.authZHandlersSupplier = getIamPolicyHandlersSupplier(projectService, metaStore);
        ControllerApi controllerApi = new ControllerApiProxy(messageChannel);
        SubscriptionService subscriptionService = new SubscriptionService(metaStore, controllerApi);
        this.subscriptionHandlers = new SubscriptionHandlers(subscriptionService, projectService);
        this.healthCheckHandler = new HealthCheckHandler();

        AuthnHandler authnHandler = new AuthnHandler(vertx, configuration);
        AuthzHandler authzHandler = new AuthzHandler(configuration);
        RequestTelemetryConfigurator requestTelemetryConfigurator =
                new RequestTelemetryConfigurator(new SpanProvider(tracer), meterRegistry);
        // payload size restriction is required for Produce APIs. But should be fine to set as default for all.
        RequestBodyHandler requestBodyHandler = new RequestBodyHandler(restOptions.getPayloadSizeMax());
        RequestBodyParser bodyParser = new RequestBodyParser();
        HierarchyHandler hierarchyHandler = new HierarchyHandler();
        this.behaviorConfigurators.put(RouteBehaviour.authenticated, authnHandler);
        this.behaviorConfigurators.put(RouteBehaviour.hasBody, (route, routeDef) -> route.handler(requestBodyHandler));
        this.behaviorConfigurators.put(RouteBehaviour.parseBody, bodyParser);
        this.behaviorConfigurators.put(RouteBehaviour.addHierarchy, hierarchyHandler);
        this.behaviorConfigurators.put(RouteBehaviour.authorized, authzHandler);
        this.behaviorConfigurators.put(RouteBehaviour.telemetry, requestTelemetryConfigurator);
    }

    private static Supplier<IamPolicyHandlers> getIamPolicyHandlersSupplier(
            ProjectService projectService, MetaStore metaStore
    ) {
        return () -> {
            if (metaStore instanceof RoleBindingMetaStore rbMetaStore) {
                return new IamPolicyHandlers(
                        projectService,
                        new IamPolicyService(metaStore, rbMetaStore)
                );
            }
            throw new IllegalStateException("MetaStore is not an instance of RoleBindingMetaStore.");
        };
    }

    public List<RouteDefinition> getRouteDefinitions() {
        return Stream.of(
                        topicHandlers.get(),
                        subscriptionHandlers.get(),
                        produceHandlers.get(),
                        healthCheckHandler.get()
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public Future<String> deployVerticle(Vertx vertx, AppConfiguration configuration) {
        List<RouteDefinition> handlerDefinitions = getRouteDefinitions();
        if (shouldEnableAuthZHandlers(configuration)) {
            handlerDefinitions.addAll(authZHandlersSupplier.get().get());
        }
        return vertx.deployVerticle(
                () -> new RestVerticle(
                        handlerDefinitions,
                        behaviorConfigurators,
                        new FailureHandler(),
                        configuration.getHttpServerOptions()
                ),
                configuration.getVerticleDeploymentOptions()
        ).onFailure(t -> {
            log.error("Could not start HttpServer Verticle.", t);
            throw new VaradhiException("Failed to Deploy Rest API.", t);
        }).onSuccess(name -> log.debug("Successfully deployed the Verticle id({}).", name));
    }

    private boolean shouldEnableAuthZHandlers(AppConfiguration configuration) {
        String defaultProviderClass = DefaultAuthorizationProvider.class.getName();
        return configuration.isAuthorizationEnabled()
                && defaultProviderClass.equals(configuration.getAuthorization().getProviderClassName());
    }
}
