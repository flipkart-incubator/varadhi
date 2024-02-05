package com.flipkart.varadhi.web.routes;


import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.Getter;
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
@Getter
public class RouteDefinition {
    private final String name;
    private final HttpMethod method;
    private final String path;
    private final Set<RouteBehaviour> behaviours;
    private final LinkedHashSet<Handler<RoutingContext>> preHandlers;
    private final Handler<RoutingContext> endReqHandler;
    private final boolean blockingEndHandler;
    private final Optional<PermissionAuthorization> requiredAuthorization;
    RouteDefinition(String name, HttpMethod method, String path, Set<RouteBehaviour> behaviours,
                            LinkedHashSet<Handler<RoutingContext>> preHandlers,
                            Handler<RoutingContext> endReqHandler,
                            boolean blockingEndHandler,
                            Optional<PermissionAuthorization> requiredAuthorization) {
        this.name = name;
        this.method = method;
        this.path = path;
        this.behaviours = behaviours;
        this.preHandlers = preHandlers;
        this.endReqHandler = endReqHandler;
        this.blockingEndHandler = blockingEndHandler;
        this.requiredAuthorization = requiredAuthorization;
    }
    public static Builder get(String name, String path) {
        return new Builder(name, HttpMethod.GET, path);
    }

    public static Builder put(String name, String path) {
        return new Builder(name, HttpMethod.PUT, path);
    }

    public static Builder post(String name, String path) {
        return new Builder(name, HttpMethod.POST, path);
    }

    public static Builder delete(String name, String path) {
        return new Builder(name, HttpMethod.DELETE, path);
    }

    @RequiredArgsConstructor
    public static class Builder {
        private final String name;
        private final HttpMethod method;
        private final String path;
        private boolean unAuthenticated;
        private boolean hasBody;
        private boolean nonBlocking;
        private boolean requestLoggingOff;
        private boolean apiContextOff;
        private final LinkedHashSet<Handler<RoutingContext>> preHandlers = new LinkedHashSet<>();
        private PermissionAuthorization requiredAuthorization;

        public Builder unAuthenticated() {
            this.unAuthenticated = true;
            return this;
        }

        public Builder hasBody() {
            this.hasBody = true;
            return this;
        }

        public Builder nonBlocking() {
            this.nonBlocking = true;
            return this;
        }

        public Builder requestLoggingOff() {
            this.requestLoggingOff = true;
            return this;
        }

        public Builder apiContextOff() {
            this.apiContextOff = true;
            return this;
        }

        public Builder authorize(ResourceAction action, String resource) {
            this.requiredAuthorization = PermissionAuthorization.of(action, resource);
            return this;
        }
        public Builder preHandler(Handler<RoutingContext> preHandler) {
            if (null != preHandler) {
                this.preHandlers.add(preHandler);
            }
            return this;
        }

        public RouteDefinition build(Handler<RoutingContext> reqHandler) {
            Set<RouteBehaviour> behaviours = new LinkedHashSet<>();
            if (!unAuthenticated) {
                behaviours.add(RouteBehaviour.authenticated);
            }
            if (hasBody) {
                behaviours.add(RouteBehaviour.hasBody);
            }
            if (!requestLoggingOff) {
                behaviours.add(RouteBehaviour.requestLoggingOn);
            }
            if (!apiContextOff) {
                behaviours.add(RouteBehaviour.enableApiContext);
            }

            return new RouteDefinition(
                    name,
                    method,
                    path,
                    behaviours,
                    preHandlers,
                    reqHandler,
                    !nonBlocking,
                    null == requiredAuthorization ? Optional.empty() : Optional.of(requiredAuthorization)
            );
        }
    }

}
