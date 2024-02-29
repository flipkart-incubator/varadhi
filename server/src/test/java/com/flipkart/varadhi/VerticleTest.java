package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.web.FailureHandler;
import com.flipkart.varadhi.web.HierarchyHandler;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.google.common.collect.Sets;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;

@ExtendWith(VertxExtension.class)
public class VerticleTest {

    Vertx vertx = Vertx.vertx();
    WebClient webClient = WebClient.create(vertx);


    @Test
    public void testMultipleHandlers(VertxTestContext testContext) {

        Handler<RoutingContext> handler1 = context -> {
            context.response().setChunked(true);
            context.response().write("Hello ");
            context.next();
        };
        Handler<RoutingContext> handler2 = context -> {
            context.response().write("from Varadhi ");
            context.next();
        };
        Handler<RoutingContext> handler3 = context -> {
            context.response().end("Team!");
        };

        LinkedHashSet<Handler<RoutingContext>> handlers = Sets.newLinkedHashSet();
        handlers.add(handler1);
        handlers.add(handler2);

        RouteDefinition routeDefinition =
                RouteDefinition.get("test", "/")
                        .unAuthenticated()
//                        .apiContextOff()
                        .requestTraceAndLogOff()
                        .preHandler(handler1)
                        .preHandler(handler2)
                        .build(this::getHierarchy, handler3);
        HttpServerOptions httpServerOptions = new HttpServerOptions();
        httpServerOptions.setPort(6969);
        Map<RouteBehaviour, RouteConfigurator> configuratorMap = new HashMap<>();
        configuratorMap.put(RouteBehaviour.addHierarchy, new HierarchyHandler());
        vertx.deployVerticle(
                new RestVerticle(
                        Collections.singletonList(routeDefinition), configuratorMap, new FailureHandler(),
                        httpServerOptions
                ),
                testContext.succeeding(id -> webClient.get(6969, "localhost", "/")
                        .as(BodyCodec.string())
                        .send(testContext.succeeding(resp -> testContext.verify(() -> {
                            Assertions.assertEquals(resp.body(), "Hello from Varadhi Team!");
                            testContext.completeNow();
                        }))))
        );
    }

    public ResourceHierarchy getHierarchy(RoutingContext ctx, boolean hasBody) {
        return new Hierarchies.RootHierarchy();
    }
}
