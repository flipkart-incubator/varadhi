package com.flipkart.varadhi.web;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Route Definitions describes a route and associated behaviour. The setup code can use appropriate handlers based on
 * this definition.
 */
public record RouteDefinition(HttpMethod method, String path, Set<RouteBehaviour> behaviour,
                              Handler<RoutingContext> handler,
                              Optional<PermissionAuthorization> requiredAuthorization) {

    public enum RouteBehaviour {
        open
    }

    public interface Provider extends java.util.function.Supplier<List<RouteDefinition>> {
    }

    public record SubRoutes(String basePath, List<RouteDefinition> subRoutes) implements Provider {
        @Override
        public List<RouteDefinition> get() {
            return
                    subRoutes.stream()
                            .map(r -> new RouteDefinition(
                                    r.method, basePath + r.path, r.behaviour, r.handler, r.requiredAuthorization))
                            .collect(Collectors.toList());
        }
    }
}
