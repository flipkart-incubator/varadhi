package com.flipkart.varadhi;

import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.db.MetaStoreProvider;
import com.flipkart.varadhi.entities.ProducerFactory;
import com.flipkart.varadhi.entities.VaradhiTopicFactory;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProduceMetricProvider;
import com.flipkart.varadhi.produce.services.InternalTopicCache;
import com.flipkart.varadhi.produce.services.ProducerCache;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.MessagingStackProvider;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.web.AuthHandlers;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.HealthCheckHandler;
import com.flipkart.varadhi.web.v1.admin.TopicHandlers;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
public class VerticleDeployer {
    private final TopicHandlers topicHandlers;
    private final ProduceHandlers produceHandlers;
    private final HealthCheckHandler healthCheckHandler;
    private final Map<RouteBehaviour, RouteConfigurator> behaviorProviders = new HashMap<>();
    private HttpServer httpServer;

    public VerticleDeployer(
            Vertx vertx,
            ServerConfiguration configuration,
            MessagingStackProvider messagingStackProvider,
            MetaStoreProvider metaStoreProvider,
            MeterRegistry meterRegistry
    ) {
        String deployedRegion = configuration.getVaradhiOptions().getDeployedRegion();
        VaradhiTopicFactory varadhiTopicFactory =
                new VaradhiTopicFactory(messagingStackProvider.getStorageTopicFactory(), deployedRegion);
        VaradhiTopicService varadhiTopicService = new VaradhiTopicService(
                messagingStackProvider.getStorageTopicService(),
                metaStoreProvider.getMetaStore()
        );
        this.topicHandlers =
                new TopicHandlers(varadhiTopicFactory, varadhiTopicService, metaStoreProvider.getMetaStore());
        ProducerService producerService =
                setupProducerService(
                        messagingStackProvider, varadhiTopicService,
                        configuration.getVaradhiOptions().getProducerOptions(),
                        meterRegistry
                );
        this.produceHandlers =
                new ProduceHandlers(configuration.getVaradhiOptions().getDeployedRegion(), producerService);
        this.healthCheckHandler = new HealthCheckHandler();
        BodyHandler bodyHandler = BodyHandler.create(false);
        this.behaviorProviders.put(RouteBehaviour.authenticated, new AuthHandlers(vertx, configuration));
        this.behaviorProviders.put(RouteBehaviour.hasBody, (route, routeDef) -> route.handler(bodyHandler));
    }

    private List<RouteDefinition> getAdminRouteDefinitions() {
        return Stream.of(
                        this.topicHandlers.get(),
                        this.healthCheckHandler.get()
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<RouteDefinition> getProduceRouteDefinitions() {
        return Stream.of(
                        this.produceHandlers.get()
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public void deployVerticles(Vertx vertx, ServerConfiguration configuration) {
        Router router = Router.router(vertx);
        FailureHandler failureHandler = new FailureHandler();
        deployRestVerticle(vertx, router, failureHandler, configuration.getRestVerticleDeploymentOptions());
        deployProduceVerticle(vertx, router, failureHandler, configuration.getProduceVerticleDeploymentOptions());
        deployServer(vertx, router);
    }

    private void deployRestVerticle(
            Vertx vertx,
            Router router,
            FailureHandler failureHandler,
            DeploymentOptions deploymentOptions
    ) {
        if (!deploymentOptions.isWorker()) {
            // Rest API  should avoid complete execution on Vertx event loop thread because they are likely to be
            // blocking. Rest API need to be either offloaded from event loop via Async or need to be executed on
            // Worker Verticle or should use executeBlocking() facility.
            // Current code assumes Rest API will be executing on Worker Verticle and hence validate.
            throw new InvalidConfigException("Rest API is expected to be deployed via Worker Verticle.");
        }

        vertx.deployVerticle(
                        () -> new RestVerticle(router, getAdminRouteDefinitions(), behaviorProviders, failureHandler, null),
                        deploymentOptions
                )
                .onFailure(t -> {
                    log.error("Could not start HttpServer Verticle", t);
                    throw new VaradhiException("Failed to Deploy Rest API.", t);
                })
                .onSuccess(name -> log.debug("Successfully deployed the Verticle id({}).", name));
    }

    private void deployProduceVerticle(
            Vertx vertx,
            Router router,
            FailureHandler failureHandler,
            DeploymentOptions deploymentOptions
    ) {
        if (deploymentOptions.isWorker()) {
            throw new InvalidConfigException(
                    "Produce Verticle is expected to be deployed on Standard Verticle.");
        }

        vertx.deployVerticle(
                        () -> new ProduceVerticle(router, getProduceRouteDefinitions(), behaviorProviders, failureHandler),
                        deploymentOptions
                )
                .onFailure(t -> {
                    log.error("Could not start HttpServer Verticle", t);
                    throw new VaradhiException("Failed to Deploy Rest API.", t);
                })
                .onSuccess(name -> log.debug("Successfully deployed the Verticle id({}).", name));
    }

    //TODO:: Rest API calls are also landing on Event Loop thread after this change.
    // This needs to be debugged and Fixed.
    //TODO:: How server can be closed ? Vertx doesn't provide onClose callback like Verticle.
    private void deployServer(Vertx vertx, Router router) {
        HttpServerOptions options = new HttpServerOptions();
        options.setAlpnVersions(HttpServerOptions.DEFAULT_ALPN_VERSIONS);
        options.setUseAlpn(true);

        // TODO: create config for http server
        this.httpServer = vertx.createHttpServer(options).requestHandler(router).listen(8080, h -> {
            if (h.succeeded()) {
                log.info("HttpServer Started.");
            } else {
                log.warn("HttpServer Started Failed.");
            }
        });
    }

    //TODO::move rest and produce specific handling/initialisation to respective Verticle.
    private ProducerService setupProducerService(
            MessagingStackProvider messagingStackProvider,
            VaradhiTopicService varadhiTopicService,
            ProducerOptions producerOptions,
            MeterRegistry meterRegistry
    ) {
        ProducerFactory producerFactory = messagingStackProvider.getProducerFactory();
        ProducerCache producerCache = new ProducerCache(producerFactory, producerOptions.getProducerCacheBuilderSpec());
        InternalTopicCache internalTopicCache =
                new InternalTopicCache(varadhiTopicService, producerOptions.getTopicCacheBuilderSpec());
        ProduceMetricProvider produceMetricProvider = new ProduceMetricProvider(meterRegistry);
        return new ProducerService(producerCache, internalTopicCache, produceMetricProvider);
    }

}
