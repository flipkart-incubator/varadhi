package com.flipkart.varadhi.server.spi.authz;

import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.spi.ConfigFileResolver;
import io.vertx.core.Future;


public interface AuthorizationProvider {
    Future<Boolean> init(ConfigFileResolver resolver, AuthorizationOptions authorizationOptions);

    Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource);

    Future<Boolean> isSuperAdmin(UserContext userContext);

    class NoAuthorizationProvider implements AuthorizationProvider {

        @Override
        public Future<Boolean> init(ConfigFileResolver resolver, AuthorizationOptions authorizationOptions) {
            return Future.succeededFuture(true);
        }

        @Override
        public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
            return Future.succeededFuture(true);
        }

        @Override
        public Future<Boolean> isSuperAdmin(UserContext userContext) {
            return Future.succeededFuture(true);
        }
    }
}
