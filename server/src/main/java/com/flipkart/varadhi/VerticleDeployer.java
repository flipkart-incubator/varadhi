package com.flipkart.varadhi;

import com.flipkart.varadhi.db.MetaStoreProvider;
import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicFactory;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.services.MessagingStackProvider;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.web.AuthHandlers;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteBehaviourProvider;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.HealthCheckHandler;
import com.flipkart.varadhi.web.v1.TopicHandlers;
import io.vertx.core.DeploymentOptions;
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
    private final HealthCheckHandler healthCheckHandler;
    private final Map<RouteBehaviour, RouteBehaviourProvider> behaviorProviders = new HashMap<>();

    public VerticleDeployer(
            Vertx vertx,
            ServerConfiguration configuration,
            MessagingStackProvider messagingStackProvider,
            MetaStoreProvider metaStoreProvider
    ) {
        VaradhiTopicFactory topicFactory = new VaradhiTopicFactory(messagingStackProvider.getStorageTopicFactory());
        VaradhiTopicService topicService = new VaradhiTopicService(
                messagingStackProvider.getStorageTopicService(),
                metaStoreProvider.getMetaStore(VaradhiTopic.class)
        );
        this.topicHandlers = new TopicHandlers(topicFactory, topicService, metaStoreProvider.getMetaStore(TopicResource.class));
        this.healthCheckHandler = new HealthCheckHandler();
        BodyHandler bodyHandler = BodyHandler.create(false);
        behaviorProviders.put(RouteBehaviour.authenticated, new AuthHandlers(vertx, configuration));
        behaviorProviders.put(RouteBehaviour.hasBody, (route, routeDef) -> route.handler(bodyHandler));
    }

    private List<RouteDefinition> getRouteDefinitions() {
        return Stream.of(
                        topicHandlers.get(),
                        healthCheckHandler.get()
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public void deployVerticles(Vertx vertx, ServerConfiguration configuration) {
        deployRestVerticle(vertx, configuration.getRestVerticleDeploymentOptions());
    }

    private void deployRestVerticle(Vertx vertx, DeploymentOptions deploymentOptions) {
        if (!deploymentOptions.isWorker()) {
            // Rest API  should avoid complete execution on Vertx event loop thread because they are likely to be
            // blocking. Rest API need to be either offloaded from event loop via Async or need to be executed on
            // Worker Verticle or should use executeBlocking() facility.
            // Current code assumes Rest API will be executing on Worker Verticle and hence validate.
            log.error("Rest Verticle is expected to be deployed as Worker Verticle.");
            throw new InvalidConfigException("Rest API is expected to be deployed via Worker Verticle.");
        }

        vertx.deployVerticle(() -> new RestVerticle(getRouteDefinitions(), behaviorProviders), deploymentOptions)
                .onFailure(t -> {
                    log.error("Could not start HttpServer Verticle", t);
                    throw new VaradhiException("Failed to Deploy Rest API.", t);
                })
                .onSuccess(name -> log.debug("Successfully deployed the Verticle id({}).", name));
    }

}
