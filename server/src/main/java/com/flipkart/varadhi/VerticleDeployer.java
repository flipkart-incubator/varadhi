package com.flipkart.varadhi;

import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.core.VaradhiTopicFactory;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetricProvider;
import com.flipkart.varadhi.produce.services.InternalTopicCache;
import com.flipkart.varadhi.produce.services.ProducerCache;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.TeamService;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.services.MessagingStackProvider;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.flipkart.varadhi.web.AuthHandlers;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.HealthCheckHandler;
import com.flipkart.varadhi.web.v1.admin.OrgHandlers;
import com.flipkart.varadhi.web.v1.admin.ProjectHandlers;
import com.flipkart.varadhi.web.v1.admin.TeamHandlers;
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
    private final OrgHandlers orgHandlers;
    private final TeamHandlers teamHandlers;
    private final ProjectHandlers projectHandlers;
    private final Map<RouteBehaviour, RouteConfigurator> behaviorConfigurators = new HashMap<>();


    public VerticleDeployer(
            String hostName,
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
        MetaStore metaStore = metaStoreProvider.getMetaStore();
        this.topicHandlers =
                new TopicHandlers(varadhiTopicFactory, varadhiTopicService, metaStore);
        ProducerService producerService =
                setupProducerService(
                        messagingStackProvider, varadhiTopicService,
                        configuration.getVaradhiOptions().getProducerOptions(),
                        meterRegistry
                );
        this.orgHandlers = new OrgHandlers(new OrgService(metaStore));
        this.teamHandlers = new TeamHandlers(new TeamService(metaStore));
        this.projectHandlers = new ProjectHandlers(new ProjectService(metaStore));
        
        this.produceHandlers =
                new ProduceHandlers(hostName, configuration.getVaradhiOptions(), producerService);
        this.healthCheckHandler = new HealthCheckHandler();
        BodyHandler bodyHandler = BodyHandler.create(false);
        // payload size restriction is required for Produce APIs. But should be fine to set as default for all.
        bodyHandler.setBodyLimit(configuration.getVaradhiOptions().getPayloadSizeMax());
        this.behaviorConfigurators.put(RouteBehaviour.authenticated, new AuthHandlers(vertx, configuration));
        this.behaviorConfigurators.put(RouteBehaviour.hasBody, (route, routeDef) -> route.handler(bodyHandler));
    }

    private List<RouteDefinition> getDefinitions() {
        return Stream.of(
                        orgHandlers.get(),
                        teamHandlers.get(),
                        projectHandlers.get(),
                        topicHandlers.get(),
                        produceHandlers.get(),
                        healthCheckHandler.get()
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
                                behaviorConfigurators,
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
        ProducerFactory<StorageTopic> producerFactory = messagingStackProvider.getProducerFactory();
        ProducerCache producerCache = new ProducerCache(producerFactory, producerOptions.getProducerCacheBuilderSpec());
        InternalTopicCache internalTopicCache =
                new InternalTopicCache(varadhiTopicService, producerOptions.getTopicCacheBuilderSpec());
        ProducerMetricProvider producerMetricProvider =
                new ProducerMetricProvider(producerOptions.isMetricEnabled(), meterRegistry);
        return new ProducerService(producerCache, internalTopicCache, producerMetricProvider);
    }

}
