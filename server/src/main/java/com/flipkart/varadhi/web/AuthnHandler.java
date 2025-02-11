package com.flipkart.varadhi.web;

import com.flipkart.varadhi.authn.AnonymousAuthenticationHandler;
import com.flipkart.varadhi.authn.CustomAuthenticationHandler;
import com.flipkart.varadhi.authn.JWTAuthenticationHandler;
import com.flipkart.varadhi.authn.UserHeaderAuthenticationHandler;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

public class AuthnHandler implements RouteConfigurator {
    private final AuthenticationHandlerWrapper authenticationHandler;

    public AuthnHandler(Vertx vertx, AppConfiguration configuration) throws InvalidConfigException {

        AuthenticationConfig authenticationConfig = configuration.getAuthentication();
        authenticationHandler = new AuthenticationHandlerWrapper(

                switch (configuration.getAuthentication().getMechanism()) {
                    case custom -> new CustomAuthenticationHandler().provideHandler(vertx, authenticationConfig);
                    case anonymous -> new AnonymousAuthenticationHandler().provideHandler(vertx, authenticationConfig);
                }
        );

    }

    public void configure(Route route, RouteDefinition routeDef) {
        if (authenticationHandler != null) {
            route.handler(authenticationHandler);
        }
    }

    @AllArgsConstructor
    static class AuthenticationHandlerWrapper implements Handler<RoutingContext> {
        private final Handler<RoutingContext> wrappedHandler;

        @Override
        public void handle(RoutingContext ctx) {
            wrappedHandler.handle(ctx);
        }
    }
}
