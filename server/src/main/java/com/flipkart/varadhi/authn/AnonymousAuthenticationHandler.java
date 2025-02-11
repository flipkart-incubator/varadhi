package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.config.AuthenticationConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;

public class AnonymousAuthenticationHandler {

    public AuthenticationHandler provideHandler(Vertx vertx, AuthenticationConfig authenticationConfig) {

        return SimpleAuthenticationHandler.create().authenticate(ctx ->
             Future.succeededFuture(User.fromName("anonymous"))
        );
    }
}
