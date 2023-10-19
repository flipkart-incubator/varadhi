package com.flipkart.varadhi.web;

import com.flipkart.varadhi.authz.AuthorizationProvider;
import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.auth.ResourceAction;
import com.flipkart.varadhi.entities.UserContext;
import com.flipkart.varadhi.entities.VertxUserContext;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

    public AuthorizationHandler build(PermissionAuthorization requiredAuthorization) {
        return new AuthorizationHandler(requiredAuthorization);
    }

    @AllArgsConstructor
    class AuthorizationHandler implements Handler<RoutingContext> {

        private final PermissionAuthorization requiredAuthorization;

        @Override
        public void handle(RoutingContext ctx) {
            UserContext user = ctx.user() == null ? null : new VertxUserContext(ctx.user());
            Function<String, String> env =
                    v -> resolveVariable(v, ctx.pathParams(), ctx.request().params(), ctx.request().headers());
            authorize(user, env).onFailure(ctx::fail).onSuccess(result -> ctx.next());
        }

        Future<Void> authorize(UserContext userContext, Function<String, String> env) {

            if (userContext == null) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the request is not authenticated"));
            }

            if (userContext.isExpired()) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the user / token has been expired"));
            }

            if (superUsers.contains(userContext.getSubject())) {
                return Future.succeededFuture();
            }

            ResourceAction action = requiredAuthorization.action();
            String resource = requiredAuthorization.resource().resolve(env);
            return provider.isAuthorized(userContext, action, resource)
                    .compose(authorized -> {
                        if (!authorized) {
                            return Future.failedFuture(new HttpException(
                                    HTTP_FORBIDDEN,
                                    "user is not authorized to perform action '" + action.toString() +
                                            "' on resource '" +
                                            resource + "'"
                            ));
                        } else {
                            return Future.succeededFuture();
                        }
                    }, t -> Future.failedFuture(
                            new HttpException(HTTP_INTERNAL_ERROR, "failed to get user authorization")));
        }

        String resolveVariable(
                String variable, Map<String, String> pathParams, MultiMap queryParams, MultiMap headers
        ) {
            String value = pathParams.get(variable);
            if (value == null) {
                value = queryParams.get(variable);
            }
            if (value == null) {
                value = headers.get(variable);
            }
            return value;
        }
    }
}
