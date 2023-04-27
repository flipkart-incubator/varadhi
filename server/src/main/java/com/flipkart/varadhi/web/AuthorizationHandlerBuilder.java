package com.flipkart.varadhi.web;

import com.flipkart.varadhi.auth.AuthorizationProvider;
import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.auth.ResourceAction;
import com.flipkart.varadhi.auth.user.VertxUserContext;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.net.HttpURLConnection.*;

@Slf4j
public class AuthorizationHandlerBuilder {

    private final List<String> superUsers = new ArrayList<>();

    private final AuthorizationProvider provider;

    public AuthorizationHandlerBuilder(List<String> superUsers, AuthorizationProvider provider) {
        if (superUsers != null) {
            this.superUsers.addAll(superUsers);
        }
        this.provider = provider != null ? provider : new AuthorizationProvider.NoAuthorizationProvider();
    }

    public Handler<RoutingContext> build(RouteDefinition routeDefinition) {
        return new AuthorizationHandler(routeDefinition);
    }

    private class AuthorizationHandler implements Handler<RoutingContext> {

        private final RouteDefinition routeDef;

        AuthorizationHandler(RouteDefinition routeDef) {
            this.routeDef = routeDef;
        }

        @Override
        public void handle(RoutingContext ctx) {
            // user needs to be authenticated, if authorization is required
            if (routeDef.requiredAuthorization().isEmpty()) {
                ctx.next();
                return;
            }

            User user = ctx.user();
            if (user == null) {
                ctx.fail(new HttpException(HTTP_UNAUTHORIZED, "the request is not authenticated"));
                return;
            }

            if (user.expired()) {
                ctx.fail(new HttpException(HTTP_UNAUTHORIZED, "the user / token has been expired"));
                return;
            }

            VertxUserContext userContext = new VertxUserContext(user);
            if (superUsers.contains(userContext.getSubject())) {
                ctx.next();
                return;
            }

            PermissionAuthorization requiredAuth = routeDef.requiredAuthorization().get();
            ResourceAction action = requiredAuth.action();
            String resource = requiredAuth.resource().resolve(
                    v -> resolveVariable(v, ctx.pathParams(), ctx.request().params(), ctx.request().headers()));
            provider.isAuthorized(userContext, action, resource).onComplete(ar -> {
                if (ar.failed()) {
                    ctx.fail(new HttpException(HTTP_INTERNAL_ERROR, "failed to get user authorization"));
                }

                if (!ar.result()) {
                    ctx.fail(new HttpException(
                            HTTP_FORBIDDEN,
                            "user is not authorized to perform action '" + action.toString() + "' on resource '" +
                                    resource + "'"
                    ));
                } else {
                    ctx.next();
                }
            });
        }

        String resolveVariable(
                String variable, Map<String, String> pathParams, MultiMap queryparams, MultiMap headers
        ) {
            String value = pathParams.get(variable);
            if (value == null) {
                value = queryparams.get(variable);
            }
            if (value == null) {
                value = headers.get(variable);
            }
            return value;
        }
    }
}
