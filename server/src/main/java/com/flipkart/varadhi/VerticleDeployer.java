package com.flipkart.varadhi;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.core.VaradhiTopicFactory;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetrics;
import com.flipkart.varadhi.produce.otel.ProducerMetricsImpl;
import com.flipkart.varadhi.produce.otel.ProducerMetricsNoOpImpl;
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
public abstract class VerticleDeployer {
    private final TopicHandlers topicHandlers;
    private final ProduceHandlers produceHandlers;
    private final HealthCheckHandler healthCheckHandler;

    private final Map<RouteBehaviour, RouteConfigurator> behaviorConfigurators = new HashMap<>();

    protected final OrgService orgService;
    protected final TeamService teamService;
    protected final ProjectService projectService;

    public OrgService getOrgService() {
        return orgService;
    }

    public TeamService getTeamService() {
        return teamService;
    }

    public ProjectService getProjectService() {
        return projectService;
    }

    public VerticleDeployer(
            String hostName,
            Vertx vertx,
            ServerConfiguration configuration,
            MessagingStackProvider messagingStackProvider,
            MetaStoreProvider metaStoreProvider,
            MeterRegistry meterRegistry
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
        this.topicHandlers =
                new TopicHandlers(varadhiTopicFactory, varadhiTopicService, metaStore);
        ProducerService producerService =
                setupProducerService(
                        configuration.getProducerOptions(), messagingStackProvider.getProducerFactory(),
                        varadhiTopicService, meterRegistry
                );
        this.projectService =
                new ProjectService(metaStore, restOptions.getProjectCacheBuilderSpec(), meterRegistry);
        this.orgService = new OrgService(metaStore);
        this.teamService = new TeamService(metaStore);

        this.produceHandlers =
                new ProduceHandlers(hostName, configuration.getRestOptions(), producerService, projectService);
        this.healthCheckHandler = new HealthCheckHandler();
        BodyHandler bodyHandler = BodyHandler.create(false);
        // payload size restriction is required for Produce APIs. But should be fine to set as default for all.
        bodyHandler.setBodyLimit(configuration.getRestOptions().getPayloadSizeMax());
        this.behaviorConfigurators.put(RouteBehaviour.authenticated, new AuthHandlers(vertx, configuration));
        this.behaviorConfigurators.put(RouteBehaviour.hasBody, (route, routeDef) -> route.handler(bodyHandler));
    }

    public List<RouteDefinition> getRouteDefinitions() {
        return Stream.of(
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
                                getRouteDefinitions(),
                                behaviorConfigurators,
                                new FailureHandler(),
                                configuration.getHttpServerOptions()
                        ),
                        configuration.getVerticleDeploymentOptions()
                )
                .onFailure(t -> {
                    log.error("Could not start HttpServer Verticle.", t);
                    throw new VaradhiException("Failed to Deploy Rest API.", t);
                })
                .onSuccess(name -> log.debug("Successfully deployed the Verticle id({}).", name));
    }

    private ProducerService setupProducerService(
            ProducerOptions producerOptions,
            ProducerFactory<StorageTopic> producerFactory,
            VaradhiTopicService varadhiTopicService,
            MeterRegistry meterRegistry
    ) {
        ProducerMetrics producerMetrics = producerOptions.isMetricEnabled() ? new ProducerMetricsImpl(meterRegistry) :
                new ProducerMetricsNoOpImpl();
        return new ProducerService(
                producerOptions, producerFactory, producerMetrics, varadhiTopicService, meterRegistry);
    }

}
