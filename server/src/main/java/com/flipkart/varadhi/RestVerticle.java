package com.flipkart.varadhi;

import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.RouteDefinition;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestVerticle extends AbstractVerticle {

    private final ServerConfiguration configuration;

    private final CoreServices coreServices;

    private HttpServer httpServer;

    public RestVerticle(ServerConfiguration configuration, CoreServices coreServices) {
        this.configuration = configuration;
        this.coreServices = coreServices;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        log.info("HttpServer Starting.");

        Router router = Router.router(vertx);

        FailureHandler failureHandler = new FailureHandler();
        for (RouteDefinition def : coreServices.getRouteDefinitions()) {
            Route route = router.route().method(def.method()).path(def.path());
            def.behaviours().stream().forEach(d -> d.Configure(route, def, coreServices));
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
