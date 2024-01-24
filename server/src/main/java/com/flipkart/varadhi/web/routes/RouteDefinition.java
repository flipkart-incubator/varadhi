package com.flipkart.varadhi.web.routes;


import com.flipkart.varadhi.auth.PermissionAuthorization;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;


/**
 * Route Definitions describes a route and associated behaviours. The setup code can use appropriate handlers based on
 * this definition.
 */

@Slf4j
//TODO: Add metric Name as param when this is approved
public record RouteDefinition(HttpMethod method, String path, Set<RouteBehaviour> behaviours,
                              LinkedHashSet<Handler<RoutingContext>> preHandlers,
                              Handler<RoutingContext> endReqHandler,
                              boolean blockingEndHandler,
                              Optional<PermissionAuthorization> requiredAuthorization) {

    public static Builder get(String path) {
        return new Builder(HttpMethod.GET, path);
    }

    public static Builder put(String path) {
        return new Builder(HttpMethod.PUT, path);
    }

    public static Builder post(String path) {
        return new Builder(HttpMethod.POST, path);
    }

    public static Builder delete(String path) {
        return new Builder(HttpMethod.DELETE, path);
    }

    @RequiredArgsConstructor
    public static class Builder {
        private final HttpMethod method;
        private final String path;
        private boolean authenticated;
        private boolean hasBody;
        private boolean blocking;

        public Builder authenticated() {
            this.authenticated = true;
            return this;
        }

        public Builder hasBody() {
            this.hasBody = true;
            return this;
        }

        public Builder blocking() {
            this.blocking = true;
            return this;
        }

        public RouteDefinition build(Handler<RoutingContext> reqHandler) {
            Set<RouteBehaviour> behaviours = new LinkedHashSet<>();
            if (authenticated) {
                behaviours.add(RouteBehaviour.authenticated);
            }
            if (hasBody) {
                behaviours.add(RouteBehaviour.hasBody);
            }

            return new RouteDefinition(
                    method,
                    path,
                    behaviours,
                    new LinkedHashSet<>(),
                    reqHandler,
                    blocking,
                    Optional.empty()
            );
        }
    }

    //This can be taken as params
    public String getMetricName() {
        return "dummy_metric_name";
    }

}
