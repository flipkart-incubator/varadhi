package com.flipkart.varadhi.web;

import com.flipkart.varadhi.auth.AuthenticationOptions;
import com.flipkart.varadhi.authz.AuthorizationOptions;
import com.flipkart.varadhi.authz.AuthorizationProvider;
import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

public class AuthHandlers implements RouteConfigurator {
    private final Handler<RoutingContext> authenticationHandler;
    private final AuthorizationHandlerBuilder authorizationHandlerBuilder;

    public AuthHandlers(Vertx vertx, ServerConfiguration configuration) throws InvalidConfigException {
        if (configuration.isAuthenticationEnabled()) {
            authenticationHandler =
                    switch (configuration.getAuthentication().getMechanism()) {
                        case jwt -> createJWTHandler(
                                vertx,
                                configuration.getAuthentication().asConfig(AuthenticationOptions.JWTConfig.class)
                        );
                        case dummy -> createDummyHandler();
                    };
        } else {
            authenticationHandler = null;
        }

        if (configuration.isAuthenticationEnabled() && configuration.isAuthorizationEnabled()) {
            authorizationHandlerBuilder = createAuthorizationHandler(configuration);
        } else {
            authorizationHandlerBuilder = null;
        }
    }

    public void configure(Route route, RouteDefinition routeDef) {
        if (authenticationHandler != null) {
            route.handler(authenticationHandler);
        }

        if (authorizationHandlerBuilder != null && routeDef.requiredAuthorization().isPresent()) {
            route.handler(authorizationHandlerBuilder.build(routeDef.requiredAuthorization().get()));
        }
    }

    AuthorizationHandlerBuilder createAuthorizationHandler(ServerConfiguration configuration) {
        if (configuration.isAuthorizationEnabled()) {
            AuthorizationProvider authorizationProvider = getAuthorizationProvider(configuration);
            return new AuthorizationHandlerBuilder(configuration.getAuthorization()
                    .getSuperUsers(), authorizationProvider);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private AuthorizationProvider getAuthorizationProvider(ServerConfiguration configuration) {
        String providerClassName = configuration.getAuthorization().getProviderClassName();
        if (StringUtils.isNotBlank(providerClassName)) {
            try {
                Class<? extends AuthorizationProvider> clazz =
                        (Class<? extends AuthorizationProvider>) Class.forName(providerClassName);
                return createAuthorizationProvider(clazz, configuration.getAuthorization());
            } catch (ClassNotFoundException | ClassCastException e) {
                throw new InvalidConfigException(e);
            }
        }
        return new AuthorizationProvider.NoAuthorizationProvider();
    }

    AuthorizationProvider createAuthorizationProvider(
            Class<? extends AuthorizationProvider> clazz, AuthorizationOptions options
    ) throws InvalidConfigException {
        try {
            AuthorizationProvider provider = clazz.getDeclaredConstructor().newInstance();
            provider.init(options);
            return provider;
        } catch (Exception e) {
            throw new InvalidConfigException(e);
        }
    }

    JWTAuthHandler createJWTHandler(Vertx vertx, AuthenticationOptions.JWTConfig config) {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder().uri(new URI(config.getJwksUrl())).build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HTTP_OK) {
                throw new HttpException(response.statusCode(), response.body());
            }

            JsonArray jwkKeys = new JsonObject(response.body()).getJsonArray("keys");
            if (null == jwkKeys) {
                throw new VaradhiException(
                        String.format("Invalid jwks url %s response. No jwk keys found.", config.getJwksUrl()));
            }

            JWTAuthOptions jwtAuthOptions = new JWTAuthOptions();
            for (int i = 0; i < jwkKeys.size(); ++i) {
                jwtAuthOptions.addJwk(jwkKeys.getJsonObject(i));
            }
            jwtAuthOptions.setJWTOptions(config.getOptions());
            JWTAuth provider = JWTAuth.create(vertx, jwtAuthOptions);
            return JWTAuthHandler.create(provider);
        } catch (Exception e) {
            throw new VaradhiException("Failed to Initialise JWT Authentication handler.", e);
        }
    }

    AuthenticationHandler createDummyHandler() {
        return SimpleAuthenticationHandler.create().authenticate(ctx -> {
            String userName = ctx.request().getHeader("X-Auth-Token");
            if (StringUtils.isBlank(userName)) {
                return Future.failedFuture(new HttpException(HTTP_UNAUTHORIZED, "no user details present"));
            }
            return Future.succeededFuture(User.fromName(userName));
        });
    }
}
