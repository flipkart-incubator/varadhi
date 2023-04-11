package com.flipkart.varadhi;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Set;


/**
 * Route Definitions describes a route and associated behaviour. The setup code can setup appropriate handlers based on
 * this definition.
 */
public record RouteDefinition(HttpMethod method, String path, Set<RouteBehaviour> behaviour,
                              Handler<RoutingContext> handler) {
    public enum RouteBehaviour {
        open
    }

    public interface Provider extends java.util.function.Supplier<List<RouteDefinition>> {
    }

    public RouteDefinition withPath(String newPath) {
        return new RouteDefinition(method, newPath, behaviour, handler);
    }
}
