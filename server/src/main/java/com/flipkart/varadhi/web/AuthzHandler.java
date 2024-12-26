package com.flipkart.varadhi.web;

import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.spi.authz.AuthorizationOptions;
import com.flipkart.varadhi.spi.authz.AuthorizationProvider;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;
import org.apache.commons.lang3.StringUtils;

public class AuthzHandler implements RouteConfigurator {
    private final AuthorizationHandlerBuilder authorizationHandlerBuilder;
    private final SuperUserHandler superUserHandler;

    public AuthzHandler(AppConfiguration configuration, ConfigFileResolver resolver) throws InvalidConfigException {
        if (configuration.isAuthenticationEnabled() && configuration.isAuthorizationEnabled()) {
            authorizationHandlerBuilder = createAuthorizationHandler(configuration, resolver);
        } else {
            authorizationHandlerBuilder = null;
        }
        superUserHandler = new SuperUserHandler(configuration);
    }

    public void configure(Route route, RouteDefinition routeDef) {
        if (authorizationHandlerBuilder != null && !routeDef.getAuthorizeOnActions().isEmpty()) {
            routeDef.getAuthorizeOnActions()
                    .forEach(action -> route.handler(authorizationHandlerBuilder.build(action)));
        }
        superUserHandler.configure(route, routeDef);
    }

    AuthorizationHandlerBuilder createAuthorizationHandler(AppConfiguration configuration, ConfigFileResolver resolver) {
        if (configuration.isAuthorizationEnabled()) {
            AuthorizationProvider authorizationProvider = getAuthorizationProvider(configuration, resolver);
            return new AuthorizationHandlerBuilder(configuration.getAuthorization()
                    .getSuperUsers(), authorizationProvider);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private AuthorizationProvider getAuthorizationProvider(AppConfiguration configuration, ConfigFileResolver resolver) {
        String providerClassName = configuration.getAuthorization().getProviderClassName();
        if (StringUtils.isNotBlank(providerClassName)) {
            try {
                Class<? extends AuthorizationProvider> clazz =
                        (Class<? extends AuthorizationProvider>) Class.forName(providerClassName);
                return createAuthorizationProvider(clazz, configuration.getAuthorization(), resolver);
            } catch (ClassNotFoundException | ClassCastException e) {
                throw new InvalidConfigException(e);
            }
        }
        throw new InvalidConfigException("AuthorizationProvider class not configured.");
    }

    AuthorizationProvider createAuthorizationProvider(
            Class<? extends AuthorizationProvider> clazz, AuthorizationOptions options, ConfigFileResolver resolver
    ) throws InvalidConfigException {
        try {
            AuthorizationProvider provider = clazz.getDeclaredConstructor().newInstance();
            provider.init(resolver, options);
            return provider;
        } catch (Exception e) {
            throw new InvalidConfigException(e);
        }
    }
}
