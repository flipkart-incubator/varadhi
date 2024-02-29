package com.flipkart.varadhi.web;

import com.flipkart.varadhi.authz.AuthorizationProvider;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.VertxUserContext;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.UserContext;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static java.net.HttpURLConnection.*;

@Slf4j
public class AuthorizationHandlerBuilder {

    private final List<String> superUsers = new ArrayList<>();

    private final AuthorizationProvider provider;

    public AuthorizationHandlerBuilder(List<String> superUsers, AuthorizationProvider provider) {
        if (superUsers != null) {
            this.superUsers.addAll(superUsers);
        }
        this.provider = Objects.requireNonNull(provider, "Authorization Provider is null");
    }

    public AuthorizationHandler build(ResourceAction authorizationOnResource) {
        return new AuthorizationHandler(authorizationOnResource);
    }

    @AllArgsConstructor
    class AuthorizationHandler implements Handler<RoutingContext> {

        private final ResourceAction authorizationOnAction;

        @Override
        public void handle(RoutingContext ctx) {
            UserContext user = ctx.user() == null ? null : new VertxUserContext(ctx.user());
            ResourceHierarchy resourceHierarchy = ctx.get(CONTEXT_KEY_RESOURCE_HIERARCHY);
            authorize(user, resourceHierarchy).onFailure(ctx::fail).onSuccess(result -> ctx.next());
        }

        Future<Void> authorize(
                UserContext userContext, ResourceHierarchy resourceHierarchy
        ) {

            if (userContext == null) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the request is not authenticated"));
            }

            if (userContext.isExpired()) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the user / token has been expired"));
            }

            if (superUsers.contains(userContext.getSubject())) {
                return Future.succeededFuture();
            }

            ResourceAction action = authorizationOnAction;
            String resource = resourceHierarchy.getResourcePath();
            return provider.isAuthorized(userContext, action, resource)
                    .compose(authorized -> {
                        if (Boolean.FALSE.equals(authorized)) {
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

    }
}
