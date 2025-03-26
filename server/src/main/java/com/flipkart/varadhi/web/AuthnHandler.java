package com.flipkart.varadhi.web;

import com.flipkart.varadhi.common.exceptions.UnAuthenticatedException;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.spi.authn.AuthenticationHandlerProvider;

import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

import java.util.List;

import static com.flipkart.varadhi.common.Constants.ContextKeys.USER_CONTEXT;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

public class AuthnHandler implements RouteConfigurator {
    private final AuthenticationHandlerWrapper authenticationHandler;

    public AuthnHandler(Vertx vertx, AppConfiguration configuration, MeterRegistry meterRegistry)
        throws InvalidConfigException {

        AuthenticationConfig authenticationConfig = configuration.getAuthentication();
        AuthenticationHandlerProvider provider;

        try {
            Class<?> providerClass = Class.forName(authenticationConfig.getHandlerProviderClassName());
            if (!AuthenticationHandlerProvider.class.isAssignableFrom(providerClass)) {
                throw new InvalidConfigException(
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
                provider.provideHandler(vertx, JsonObject.mapFrom(authenticationConfig), Org::of, meterRegistry),
                authenticationConfig.getWhitelistedURLs()
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
        private final List<String> whitelistedURLs;

        @Override
        public void handle(RoutingContext ctx) {
            if (whitelistedURLs.contains(ctx.request().uri())) {
                ctx.next();
            } else {
                wrappedHandler.handle(ctx);
                User user = ctx.user();

                if (user != null) {
                    ctx.put(USER_CONTEXT, new UserContext() {
                        @Override
                        public String getSubject() {
                            return user.subject();
                        }

                        @Override
                        public boolean isExpired() {
                            return user.expired();
                        }
                    });
                }

                if (ctx.get(USER_CONTEXT) == null) {
                    ctx.fail(
                        UNAUTHORIZED.code(),
                        new UnAuthenticatedException("User context not found for authenticated API")
                    );
                }
            }



        }
    }
}
