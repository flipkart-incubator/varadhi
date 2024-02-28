package com.flipkart.varadhi.components;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.VerticleDeployer;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.deployment.FullDeploymentVerticleDeployer;
import com.flipkart.varadhi.deployment.LeanDeploymentVerticleDeployer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server implements Component {
    private String verticleId;
    private final AppConfiguration configuration;
    private final CoreServices coreServices;
    private final String hostName;

    public Server(String hostname, AppConfiguration configuration, CoreServices coreServices) {
        this.hostName = hostname;
        this.configuration = configuration;
        this.coreServices = coreServices;
    }

    @Override
    public Future<Void> start(Vertx vertx) {
        return deployVerticle(vertx);
    }

    @Override
    public Future<Void> shutdown(Vertx vertx) {
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
        }else{
            return Future.succeededFuture();
        }
    }
    private Future<Void> deployVerticle( Vertx vertx) {
        log.info("Verticle deployment started.");
        VerticleDeployer verticleDeployer = createVerticleDeployer(vertx);
        return verticleDeployer.deployVerticle(vertx, configuration).compose(r -> {
            log.info("Verticle() deployment completed {}.", r);
            verticleId = r;
            return Future.succeededFuture();
        }, t -> {
            log.error("Verticle() deployment failed.", t);
            return Future.failedFuture(t);
        });
    }

    private VerticleDeployer createVerticleDeployer(Vertx vertx) {
        VerticleDeployer verticleDeployer;
        if (configuration.getFeatureFlags().isLeanDeployment()) {
            verticleDeployer = new LeanDeploymentVerticleDeployer(hostName,
                    vertx,
                    configuration,
                    coreServices.getMessagingStackProvider(),
                    coreServices.getMetaStoreProvider(),
                    coreServices.getMeterRegistry(),
                    coreServices.getTracer()
            );
        } else {
            verticleDeployer = new FullDeploymentVerticleDeployer(hostName,
                    vertx,
                    configuration,
                    coreServices.getMessagingStackProvider(),
                    coreServices.getMetaStoreProvider(),
                    coreServices.getMeterRegistry(),
                    coreServices.getTracer()
            );
        }
        return verticleDeployer;
    }

}
