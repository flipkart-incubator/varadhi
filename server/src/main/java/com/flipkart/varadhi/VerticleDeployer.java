package com.flipkart.varadhi;

import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.db.MetaStoreProvider;
import com.flipkart.varadhi.entities.ProducerFactory;
import com.flipkart.varadhi.entities.VaradhiTopicFactory;
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
import io.vertx.core.Vertx;
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

    private List<RouteDefinition> getDefinitions() {
        return Stream.of(
                        this.topicHandlers.get(),
                        this.healthCheckHandler.get(),
                        this.produceHandlers.get()
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }


    public void deployVerticle(
            Vertx vertx,
            ServerConfiguration configuration
    ) {
        vertx.deployVerticle(
                        () -> new RestVerticle(
                                getDefinitions(),
                                behaviorProviders,
                                new FailureHandler(),
                                configuration.getHttpServerOptions()
                        ),
                        configuration.getVerticleDeploymentOptions()
                )
                .onFailure(t -> {
                    log.error("Could not start HttpServer Verticle", t);
                    throw new VaradhiException("Failed to Deploy Rest API.", t);
                })
                .onSuccess(name -> log.debug("Successfully deployed the Verticle id({}).", name));
    }


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
