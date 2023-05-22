package com.flipkart.varadhi.web;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Route Definitions describes a route and associated behaviours. The setup code can use appropriate handlers based on
 * this definition.
 */
public record RouteDefinition(HttpMethod method, String path, Set<RouteBehaviour> behaviours,
                              Handler<RoutingContext> handler,
                              Optional<PermissionAuthorization> requiredAuthorization) {

    public enum RouteBehaviour {
        authenticated {
            @Override
            public void Configure(Route route, RouteDefinition def, CoreServices coreServices) {
                coreServices.getAuthHandlers().configure(route, def);
            }
        },
        hasBody {
            @Override
            public void Configure(Route route, RouteDefinition def, CoreServices coreServices) {
                if (null != coreServices.getBodyHandler()) {
                    route.handler(coreServices.getBodyHandler());
                }else{
                    throw new InvalidStateException("No request body handler configured.");
                }
            }
        };

        public abstract void Configure(Route route, RouteDefinition def, CoreServices coreServices);
    }

    public interface Provider extends java.util.function.Supplier<List<RouteDefinition>> {
    }

    public record SubRoutes(String basePath, List<RouteDefinition> subRoutes) implements Provider {
        @Override
        public List<RouteDefinition> get() {
            return
                    subRoutes.stream()
                            .map(r -> new RouteDefinition(
                                    r.method, basePath + r.path, r.behaviours, r.handler, r.requiredAuthorization))
                            .collect(Collectors.toList());
        }
    }
}
