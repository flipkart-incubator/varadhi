package com.flipkart.varadhi.components.webserver;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.WebServerApiManager;
import com.flipkart.varadhi.VerticleDeployer;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.components.Component;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.deployment.FullDeploymentVerticleDeployer;
import com.flipkart.varadhi.deployment.LeanDeploymentVerticleDeployer;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.core.cluster.WebServerApi.ROUTE_WEBSERVER;

@Slf4j
public class WebServer implements Component {
    private final AppConfiguration configuration;
    private final CoreServices coreServices;
    private String verticleId;
    private final WebServerApiHandler handler;
    private final VaradhiClusterManager clusterManager;

    public WebServer(AppConfiguration configuration, CoreServices coreServices, VaradhiClusterManager clusterManager) {
        this.configuration = configuration;
        this.coreServices = coreServices;
        this.clusterManager = clusterManager;
        this.handler = new WebServerApiHandler(new WebServerApiManager());
    }

    @Override
    public Future<Void> start(Vertx vertx) {
        MessageRouter messageRouter =  clusterManager.getRouter(vertx);
        setupApiHandlers(messageRouter);
        return deployVerticle(vertx, clusterManager);
    }

    @Override
    public Future<Void> shutdown(Vertx vertx) {
        //TODO::
        // - fail health check.
        // - reject any new request for retry (may be via a custom handler ?). -- What does this mean
        // TODO:: is undeploy needed explicitly or taken care by Vertx (in close()) ?

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

    private Future<Void> deployVerticle(Vertx vertx, VaradhiClusterManager clusterManager) {
        log.info("Verticle deployment started.");
        VerticleDeployer verticleDeployer = createVerticleDeployer(vertx, clusterManager);
        return verticleDeployer.deployVerticle(vertx, configuration).compose(r -> {
            log.info("Verticle() deployment completed {}.", r);
            verticleId = r;
            return Future.succeededFuture(null);
        }, t -> {
            log.error("Verticle() deployment failed.", t);
            return Future.failedFuture(t);
        });
    }

    private VerticleDeployer createVerticleDeployer(Vertx vertx, VaradhiClusterManager clusterManager) {
        VerticleDeployer verticleDeployer;
        Tracer tracer = coreServices.getTracer("varadhi");
        if (configuration.getFeatureFlags().isLeanDeployment()) {
            verticleDeployer = new LeanDeploymentVerticleDeployer(
                    vertx,
                    configuration,
                    coreServices.getMessagingStackProvider(),
                    coreServices.getMetaStoreProvider(),
                    clusterManager,
                    coreServices.getMeterRegistry(),
                    tracer
            );
        } else {
            verticleDeployer = new FullDeploymentVerticleDeployer(
                    vertx,
                    configuration,
                    coreServices.getMessagingStackProvider(),
                    coreServices.getMetaStoreProvider(),
                    clusterManager,
                    coreServices.getMeterRegistry(),
                    tracer
            );
        }
        return verticleDeployer;
    }

    public void setupApiHandlers(MessageRouter router) {
        router.sendHandler(ROUTE_WEBSERVER, "update", handler::update);
    }

}
