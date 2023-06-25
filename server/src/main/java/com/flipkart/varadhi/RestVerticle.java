package com.flipkart.varadhi;

import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.v1.proto.MessageProducerGrpc;
import com.flipkart.varadhi.web.v1.proto.SingleMessageResponse;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.grpc.common.GrpcStatus;
import io.vertx.grpc.server.GrpcServer;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class RestVerticle extends AbstractVerticle {
    private final List<RouteDefinition> apiRoutes;
    private final Map<RouteBehaviour, RouteConfigurator> behaviorProviders;

    private HttpServer httpServer;

    public RestVerticle(
            List<RouteDefinition> apiRoutes, Map<RouteBehaviour, RouteConfigurator> behaviorProviders
    ) {
        this.apiRoutes = apiRoutes;
        this.behaviorProviders = behaviorProviders;
    }

    @Override
    public void start(Promise<Void> startPromise) {

        log.info("HttpServer Starting.");
        Router router = Router.router(vertx);

        // grpc
        GrpcServer grpcServer = GrpcServer.server(vertx);

        grpcServer.callHandler(MessageProducerGrpc.getProduceMethod(), request -> {
            request.response().status(GrpcStatus.OK).statusMessage("OK")
                    .end(SingleMessageResponse.newBuilder().setOffset("0001").build());
        });

        router.route()
                .consumes("application/grpc")
                .produces("application/grpc")
                .handler(req -> grpcServer.handle(req.request()));

        router.route()
                .consumes("application/json")
                .produces("application/json")
                .path("/topics/produce")
                .method(HttpMethod.POST)
                .handler(rc -> Extensions.RoutingContextExtension.endRequestWithResponse(rc, 200, "0001"));

        // http
        FailureHandler failureHandler = new FailureHandler();
        for (RouteDefinition def : apiRoutes) {
            Route route = router.route().method(def.method()).path(def.path());
            def.behaviours().forEach(behaviour -> {
                        RouteConfigurator behaviorProvider = behaviorProviders.getOrDefault(behaviour, null);
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
        options.setAlpnVersions(Arrays.asList(HttpVersion.HTTP_2, HttpVersion.HTTP_1_1));
        options.setUseAlpn(true);

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
