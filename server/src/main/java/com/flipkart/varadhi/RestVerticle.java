package com.flipkart.varadhi;

import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.metrices.MetricConstants;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Slf4j
@ExtensionMethod({Extensions.RoutingContextExtension.class})
public class RestVerticle extends AbstractVerticle {
    private final List<RouteDefinition> apiRoutes;
    private final Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfigurators;
    private final FailureHandler failureHandler;
    private final HttpServerOptions httpServerOptions;
    private HttpServer httpServer;

    public RestVerticle(
            List<RouteDefinition> apiRoutes,
            Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfigurators,
            FailureHandler failureHandler,
            HttpServerOptions httpServerOptions
    ) {
        this.apiRoutes = apiRoutes;
        this.routeBehaviourConfigurators = routeBehaviourConfigurators;
        this.failureHandler = failureHandler;
        this.httpServerOptions = httpServerOptions;
    }

    private void configureApiRoutes(
            Router router,
            List<RouteDefinition> apiRoutes,
            Map<RouteBehaviour, RouteConfigurator> routeBehaviourConfigurators,
            FailureHandler failureHandler
    ) {
        log.info("Configuring API routes.");
        for (RouteDefinition def : apiRoutes) {
            Route route = router.route().method(def.method()).path(def.path());
            RouteBehaviour[] behaviours = def.behaviours().toArray(new RouteBehaviour[0]);
            Arrays.sort(behaviours, Comparator.comparingInt(RouteBehaviour::getOrder));
            for (RouteBehaviour behaviour : behaviours) {
                RouteConfigurator routeConfigurator = routeBehaviourConfigurators.getOrDefault(behaviour, null);
                if (null != routeConfigurator) {
                    routeConfigurator.configure(route, def);
                } else {
                    String errMsg = String.format("No RouteBehaviourProvider configured for %s.", behaviour);
                    log.error(errMsg);
                    throw new InvalidStateException(errMsg);
                }
            }
            def.preHandlers().forEach(route::handler);
            if (def.blockingEndHandler()) {
                route.handler(wrapBlockingExecution(vertx, getHandler(def)));
            } else {
                route.handler(getHandler(def));
            }

            route.failureHandler(failureHandler);
        }
    }

    private Handler<RoutingContext> getHandler(RouteDefinition routeDefinition) {
        return ctx -> {
            ctx.request().headers().add(MetricConstants.RESOURCE_NAME, routeDefinition.getResourceName());
            routeDefinition.endReqHandler().handle(ctx);
        };
    }

    public static Handler<RoutingContext> wrapBlockingExecution(Vertx vrtx, Handler<RoutingContext> apiEndHandler) {
        // no try/catch around apiEndHandler.handle as executeBlocking does the same and fails the future.
        return ctx ->
                vrtx.executeBlocking(future -> {
                    apiEndHandler.handle(ctx);
                    future.complete();
                }, resultHandler -> {
                    if (resultHandler.succeeded()) {
                        if (null == ctx.getApiResponse()) {
                            ctx.endRequest();
                        } else {
                            ctx.endRequestWithResponse(ctx.getApiResponse());
                        }
                    } else {
                        ctx.endRequestWithException(resultHandler.cause());
                    }
                });
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);
        configureApiRoutes(router, apiRoutes, routeBehaviourConfigurators, failureHandler);
        httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(router).listen(h -> {
            if (h.succeeded()) {
                log.info("HttpServer Started.");
                startPromise.complete();
            } else {
                log.warn("HttpServer Start Failed.");
                startPromise.fail("HttpServer Start Failed.");
            }
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
