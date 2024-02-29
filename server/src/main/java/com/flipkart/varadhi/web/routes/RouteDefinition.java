package com.flipkart.varadhi.web.routes;


import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.HierarchyFunction;
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
import java.util.function.Consumer;


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
    private final Consumer<RoutingContext> bodyParser;
    private final HierarchyFunction hierarchyFunction;
    RouteDefinition(String name, HttpMethod method, String path, Set<RouteBehaviour> behaviours,
                    LinkedHashSet<Handler<RoutingContext>> preHandlers,
                    Handler<RoutingContext> endReqHandler,
                    boolean blockingEndHandler, Consumer<RoutingContext> bodyParser,
                    HierarchyFunction function, Optional<PermissionAuthorization> requiredAuthorization) {
        this.name = name;
        this.method = method;
        this.path = path;
        this.behaviours = behaviours;
        this.preHandlers = preHandlers;
        this.endReqHandler = endReqHandler;
        this.blockingEndHandler = blockingEndHandler;
        this.bodyParser = bodyParser;
        this.hierarchyFunction = function;
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
        private boolean requestTraceAndLogOff;
        private final LinkedHashSet<Handler<RoutingContext>> preHandlers = new LinkedHashSet<>();
        private Consumer<RoutingContext> bodyParser;

        private PermissionAuthorization requiredAuthorization;

        public Builder unAuthenticated() {
            this.unAuthenticated = true;
            return this;
        }

        public Builder hasBody() {
            this.hasBody = true;
            return this;
        }

        public Builder bodyParser(Consumer<RoutingContext> bodyParser) {
            this.hasBody = true;
            this.bodyParser = bodyParser;
            return this;
        }

        public Builder nonBlocking() {
            this.nonBlocking = true;
            return this;
        }

        public Builder requestTraceAndLogOff() {
            this.requestTraceAndLogOff = true;
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

        public RouteDefinition build(HierarchyFunction function, Handler<RoutingContext> reqHandler) {
            Set<RouteBehaviour> behaviours = new LinkedHashSet<>();

            if (!unAuthenticated) {
                behaviours.add(RouteBehaviour.authenticated);
            }
            if (hasBody) {
                behaviours.add(RouteBehaviour.hasBody);
                if (null != bodyParser) {
                    behaviours.add(RouteBehaviour.parseBody);
                }
            }
            behaviours.add(RouteBehaviour.addHierarchy);
            if (!requestTraceAndLogOff) {
                behaviours.add(RouteBehaviour.requestTraceAndLog);
            }
            if (null != requiredAuthorization) {
                behaviours.add(RouteBehaviour.authorized);
            }

            return new RouteDefinition(
                    name,
                    method,
                    path,
                    behaviours,
                    preHandlers,
                    reqHandler,
                    !nonBlocking,
                    bodyParser,
                    function,
                    Optional.ofNullable(requiredAuthorization)
            );
        }
    }

}
