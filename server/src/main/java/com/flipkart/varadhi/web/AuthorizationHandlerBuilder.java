package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.server.spi.authz.AuthorizationProvider;
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

import java.util.Map;
import java.util.Objects;

import static com.flipkart.varadhi.common.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static java.net.HttpURLConnection.*;

@Slf4j
public class AuthorizationHandlerBuilder {

    private final AuthorizationProvider provider;

    public AuthorizationHandlerBuilder(AuthorizationProvider provider) {
        this.provider = Objects.requireNonNull(provider, "Authorization Provider is null");
    }

    public AuthorizationHandler build(ResourceAction authorizationOnResource) {
        return new AuthorizationHandler(authorizationOnResource);
    }

    private Future<Void> authorizedInternal(UserContext userContext, ResourceAction action, String resourcePath) {
        return provider.isAuthorized(userContext, action, resourcePath).compose(authorized -> {
            if (Boolean.FALSE.equals(authorized)) {
                return Future.failedFuture(
                    new HttpException(
                        HTTP_FORBIDDEN,
                        "user is not authorized to perform action '" + action.toString() + "' on resource '"
                                        + resourcePath + "'"
                    )
                );
            } else {
                return Future.succeededFuture();
            }
        }, t -> Future.failedFuture(new HttpException(HTTP_INTERNAL_ERROR, "failed to get user authorization")));
    }

    @AllArgsConstructor
    public class AuthorizationHandler implements Handler<RoutingContext> {

        private final ResourceAction authorizationOnAction;

        @Override
        public void handle(RoutingContext ctx) {
            UserContext user = ctx.user() == null ? null : new VertxUserContext(ctx.user());
            Map<ResourceType, ResourceHierarchy> hierarchies = ctx.get(CONTEXT_KEY_RESOURCE_HIERARCHY);
            ResourceHierarchy hierarchy = hierarchies.getOrDefault(authorizationOnAction.getResourceType(), null);
            if (null == hierarchy) {
                ctx.fail(new HttpException(HTTP_INTERNAL_ERROR, "resource hierarchy is not set."));
                return;
            }
            authorize(user, hierarchy).onFailure(ctx::fail).onComplete(result -> ctx.next());
        }


        Future<Void> authorize(UserContext userContext, ResourceHierarchy resourceHierarchy) {

            if (userContext == null) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the request is not authenticated"));
            }

            if (userContext.isExpired()) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "the user / token has been expired"));
            }

            if (resourceHierarchy == null) {
                return Future.failedFuture(new HttpException(HTTP_INTERNAL_ERROR, "resource hierarchy is not set"));
            }

            String resourcePath = resourceHierarchy.getResourcePath();
            return authorizedInternal(userContext, authorizationOnAction, resourcePath);
        }
    }
}
