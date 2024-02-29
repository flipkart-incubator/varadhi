package com.flipkart.varadhi.web;

import com.flipkart.varadhi.authz.AuthorizationOptions;
import com.flipkart.varadhi.authz.AuthorizationProvider;
import com.flipkart.varadhi.config.ServerConfiguration;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;
import org.apache.commons.lang3.StringUtils;

public class AuthzHandler implements RouteConfigurator {
    private final AuthorizationHandlerBuilder authorizationHandlerBuilder;

    public AuthzHandler(ServerConfiguration configuration) throws InvalidConfigException {
        if (configuration.isAuthenticationEnabled() && configuration.isAuthorizationEnabled()) {
            authorizationHandlerBuilder = createAuthorizationHandler(configuration);
        } else {
            authorizationHandlerBuilder = null;
        }
    }

    public void configure(Route route, RouteDefinition routeDef) {
        if (authorizationHandlerBuilder != null && routeDef.getAuthorizationOnAction() != null) {
            route.handler(authorizationHandlerBuilder.build(routeDef.getAuthorizationOnAction()));
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
}
