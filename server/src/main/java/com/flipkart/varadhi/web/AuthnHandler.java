package com.flipkart.varadhi.web;

import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.spi.authn.AuthenticationHandlerProvider;

import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

public class AuthnHandler implements RouteConfigurator {
    private final AuthenticationHandlerWrapper authenticationHandler;

    public AuthnHandler(Vertx vertx, AppConfiguration configuration) throws InvalidConfigException {

        AuthenticationConfig authenticationConfig = configuration.getAuthentication();
        AuthenticationHandlerProvider provider = null;

        try {
            Class<?> providerClass = Class.forName(authenticationConfig.getHandlerProviderClassName());
            if (!AuthenticationHandlerProvider.class.isAssignableFrom(providerClass)) {
                throw new RuntimeException(
                    "Provider class " + providerClass.getName()
                                           + " does not implement AuthenticationHandlerProvider interface"
                );
            }
            provider = (AuthenticationHandlerProvider)providerClass.getDeclaredConstructor().newInstance();


        } catch (ClassNotFoundException e) {
            throw new InvalidConfigException(
                "Authentication handler provider class not found: " + authenticationConfig
                                                                                          .getHandlerProviderClassName(),
                e
            );
        } catch (Exception e) {
            throw new InvalidConfigException("Failed to create authentication handler provider", e);
        }

        try {
            authenticationHandler = new AuthenticationHandlerWrapper(
                provider.provideHandler(vertx, JsonObject.mapFrom(authenticationConfig), Org::of)
            );
        } catch (Exception e) {
            throw new InvalidConfigException("Failed to create authentication handler", e);
        }
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
