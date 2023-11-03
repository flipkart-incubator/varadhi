package com.flipkart.varadhi;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultAuthZVerticle extends AbstractVerticle {

    private final HttpServerOptions httpServerOptions;
    private HttpServer httpServer;

    public DefaultAuthZVerticle(HttpServerOptions httpServerOptions) {
        this.httpServerOptions = httpServerOptions;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);
        router.get("/").handler(ctx -> {
            ctx.response().end("Hello World!");
        });
        httpServer = vertx.createHttpServer(httpServerOptions)
                .requestHandler(router)
                .listen(handler -> {
                    if (handler.succeeded()) {
                        log.info("Authorization Server started on port {}", httpServerOptions.getPort());
                        startPromise.complete();
                    } else {
                        log.warn("Authorization Server failed to start", handler.cause());
                        startPromise.fail(handler.cause());
                    }
                });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        httpServer.close(handler -> {
            log.info("Authorization Server stopped");
            stopPromise.complete();
        });
    }
}
