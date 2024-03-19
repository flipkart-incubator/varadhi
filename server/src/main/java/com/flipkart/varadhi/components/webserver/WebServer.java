package com.flipkart.varadhi.components.webserver;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.WebServerOpManager;
import com.flipkart.varadhi.VerticleDeployer;
import com.flipkart.varadhi.cluster.ClusterManager;
import com.flipkart.varadhi.components.Component;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.core.cluster.MessageChannel;
import com.flipkart.varadhi.core.cluster.messages.SubscriptionMessage;
import com.flipkart.varadhi.deployment.FullDeploymentVerticleDeployer;
import com.flipkart.varadhi.deployment.LeanDeploymentVerticleDeployer;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebServer implements Component {
    private final AppConfiguration configuration;
    private final CoreServices coreServices;
    private String verticleId;
    private final WebServerApiHandler handler;

    public WebServer(AppConfiguration configuration, CoreServices coreServices) {
        this.configuration = configuration;
        this.coreServices = coreServices;
        this.handler = new WebServerApiHandler(new WebServerOpManager());
    }

    @Override
    public Future<Void> start(Vertx vertx, ClusterManager clusterManager) {
        MessageChannel messageChannel = clusterManager.connect(null);
        setupApiHandlers(messageChannel);
        return deployVerticle(vertx, messageChannel);
    }

    @Override
    public Future<Void> shutdown(Vertx vertx, ClusterManager clusterManager) {
        //TODO::
        // - fail health check.
        // - reject any new request for retry (may be via a custom handler ?).

        // Not taking care of concurrent execution, in general not expected for startup/shutdown.
        if (null != verticleId) {
            log.info("Undeploy verticle {}.", verticleId);
            return vertx.undeploy(verticleId).onComplete(ar -> {
                if (ar.succeeded()) {
                    verticleId = null;
                    log.info("Undeploy completed");
                } else {
                    log.error("Undeploy failed.", ar.cause());
                }
            });
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> deployVerticle(Vertx vertx, MessageChannel messageChannel) {
        log.info("Verticle deployment started.");
        VerticleDeployer verticleDeployer = createVerticleDeployer(vertx, messageChannel);
        return verticleDeployer.deployVerticle(vertx, configuration).compose(r -> {
            log.info("Verticle() deployment completed {}.", r);
            verticleId = r;
            return Future.succeededFuture();
        }, t -> {
            log.error("Verticle() deployment failed.", t);
            return Future.failedFuture(t);
        });
    }

    private VerticleDeployer createVerticleDeployer(Vertx vertx, MessageChannel messageChannel) {
        VerticleDeployer verticleDeployer;
        Tracer tracer = coreServices.getTracer("varadhi");
        if (configuration.getFeatureFlags().isLeanDeployment()) {
            verticleDeployer = new LeanDeploymentVerticleDeployer(
                    vertx,
                    configuration,
                    coreServices.getMessagingStackProvider(),
                    coreServices.getMetaStoreProvider(),
                    messageChannel,
                    coreServices.getMeterRegistry(),
                    tracer
            );
        } else {
            verticleDeployer = new FullDeploymentVerticleDeployer(
                    vertx,
                    configuration,
                    coreServices.getMessagingStackProvider(),
                    coreServices.getMetaStoreProvider(),
                    messageChannel,
                    coreServices.getMeterRegistry(),
                    tracer
            );
        }
        return verticleDeployer;
    }
    public void setupApiHandlers(MessageChannel messageChannel) {
        messageChannel.register("webserver", SubscriptionMessage.class, handler::update);
    }

}
