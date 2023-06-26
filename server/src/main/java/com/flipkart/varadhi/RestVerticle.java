package com.flipkart.varadhi;

import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteBehaviourProvider;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class RestVerticle extends AbstractVerticle {
    private final List<RouteDefinition> apiRoutes;
    private final Map<RouteBehaviour, RouteBehaviourProvider> behaviorProviders;

    private HttpServer httpServer;

    public RestVerticle(
            List<RouteDefinition> apiRoutes, Map<RouteBehaviour, RouteBehaviourProvider> behaviorProviders
    ) {
        this.apiRoutes = apiRoutes;
        this.behaviorProviders = behaviorProviders;
    }

    @Override
    public void start(Promise<Void> startPromise) {

        log.info("HttpServer Starting.");
        Router router = Router.router(vertx);

        FailureHandler failureHandler = new FailureHandler();
        for (RouteDefinition def : apiRoutes) {
            Route route = router.route().method(def.method()).path(def.path());
            def.behaviours().forEach(behaviour -> {
                        RouteBehaviourProvider behaviorProvider = behaviorProviders.getOrDefault(behaviour, null);
                        if (null != behaviorProvider) {
                            behaviorProvider.configure(route, def);
                        } else {
                            String errMsg = String.format("No RouteBehaviourProvider configured for %s.", behaviour);
                            log.error(errMsg);
                            throw new InvalidStateException(errMsg);
                        }
                        behaviorProvider.configure(route, def);
                    }
            );
            route.handler(def.handler());
            route.failureHandler(failureHandler);
        }

        HttpServerOptions options = new HttpServerOptions();
        // TODO: why?
        options.setDecompressionSupported(false);

        // TODO: create config for http server
        httpServer = vertx.createHttpServer(options).requestHandler(router).listen(8080, h -> {
            if (h.succeeded()) {
                log.info("HttpServer Started.");
            } else {
                log.warn("HttpServer Started Failed.");
            }
            startPromise.handle(h.map((Void) null));
        });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("HttpServer Stopping.");
        httpServer.close(h -> {
            log.info("HttpServer Stopped.");
            stopPromise.complete();
        });
    }
}
