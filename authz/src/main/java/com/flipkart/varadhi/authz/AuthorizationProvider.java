package com.flipkart.varadhi.authz;

import com.flipkart.varadhi.entities.ResourceAction;
import com.flipkart.varadhi.config.AuthorizationOptions;
import com.flipkart.varadhi.entities.UserContext;
import io.vertx.core.Future;

public interface AuthorizationProvider {
    Future<Boolean> init(AuthorizationOptions authorizationOptions);

    Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource);

    class NoAuthorizationProvider implements AuthorizationProvider {

        @Override
        public Future<Boolean> init(AuthorizationOptions authorizationOptions) {
            return Future.succeededFuture(true);
        }

        @Override
        public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
            return Future.succeededFuture(false);
        }
    }
}
