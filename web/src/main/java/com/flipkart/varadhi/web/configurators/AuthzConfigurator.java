package com.flipkart.varadhi.web.configurators;

import com.flipkart.varadhi.spi.ConfigFileResolver;
import com.flipkart.varadhi.web.spi.authz.AuthorizationOptions;
import com.flipkart.varadhi.web.spi.authz.AuthorizationProvider;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.web.AuthorizationHandlerBuilder;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.ext.web.Route;
import org.apache.commons.lang3.StringUtils;

public class AuthzConfigurator implements RouteConfigurator {
    private final AuthorizationHandlerBuilder authorizationHandlerBuilder;

    public AuthzConfigurator(AppConfiguration configuration, ConfigFileResolver resolver, MeterRegistry meterRegistry)
        throws InvalidConfigException {
        if (configuration.getAuthorization().isEnabled()) {
            authorizationHandlerBuilder = createAuthorizationHandler(configuration, resolver, meterRegistry);
        } else {
            authorizationHandlerBuilder = null;
        }
    }

    public void configure(Route route, RouteDefinition routeDef) {
        if (authorizationHandlerBuilder != null && !routeDef.getAuthorizeOnActions().isEmpty()) {
            routeDef.getAuthorizeOnActions()
                    .forEach(action -> route.handler(authorizationHandlerBuilder.build(action)));
        }
    }

    AuthorizationHandlerBuilder createAuthorizationHandler(
        AppConfiguration configuration,
        ConfigFileResolver resolver,
        MeterRegistry meterRegistry
    ) {
        if (configuration.getAuthorization().isEnabled()) {
            AuthorizationProvider authorizationProvider = getAuthorizationProvider(
                configuration,
                resolver,
                meterRegistry
            );
            return new AuthorizationHandlerBuilder(authorizationProvider, meterRegistry);
        } else {
            return null;
        }
    }

    @SuppressWarnings ("unchecked")
    private AuthorizationProvider getAuthorizationProvider(
        AppConfiguration configuration,
        ConfigFileResolver resolver,
        MeterRegistry meterRegistry
    ) {
        String providerClassName = configuration.getAuthorization().getProviderClassName();
        if (StringUtils.isNotBlank(providerClassName)) {
            try {
                Class<? extends AuthorizationProvider> clazz = (Class<? extends AuthorizationProvider>)Class.forName(
                    providerClassName
                );
                return createAuthorizationProvider(clazz, configuration.getAuthorization(), resolver, meterRegistry);
            } catch (ClassNotFoundException | ClassCastException e) {
                throw new InvalidConfigException(e);
            }
        }
        throw new InvalidConfigException("AuthorizationProvider class not configured.");
    }

    AuthorizationProvider createAuthorizationProvider(
        Class<? extends AuthorizationProvider> clazz,
        AuthorizationOptions options,
        ConfigFileResolver resolver,
        MeterRegistry meterRegistry
    ) throws InvalidConfigException {
        try {
            AuthorizationProvider provider = clazz.getDeclaredConstructor().newInstance();
            provider.init(resolver, options, meterRegistry);
            return provider;
        } catch (Exception e) {
            throw new InvalidConfigException(e);
        }
    }
}
