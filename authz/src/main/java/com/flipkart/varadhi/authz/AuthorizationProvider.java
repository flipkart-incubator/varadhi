package com.flipkart.varadhi.authz;

import com.flipkart.varadhi.auth.ResourceAction;
import com.flipkart.varadhi.entities.UserContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public interface AuthorizationProvider {
    Future<Boolean> init(Vertx vertx, AuthorizationOptions authorizationOptions);

    Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource);

    class NoAuthorizationProvider implements AuthorizationProvider {

        @Override
        public Future<Boolean> init(Vertx vertx, AuthorizationOptions authorizationOptions) {
            return Future.succeededFuture(true);
        }

        @Override
        public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
            return Future.succeededFuture(false);
        }
    }
}
