package com.flipkart.varadhi.authn;


import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.server.spi.RequestContext;
import com.flipkart.varadhi.server.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.server.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.server.spi.authn.AuthenticationProvider;
import com.flipkart.varadhi.server.spi.utils.OrgResolver;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import jakarta.ws.rs.BadRequestException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

import static com.flipkart.varadhi.common.Constants.ContextKeys.USER_CONTEXT;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class CustomAuthenticationHandler implements AuthenticationHandler, AuthenticationHandlerProvider {
    private static final String DEFAULT_ORG = "SYSTEM";
    private AuthenticationProvider authenticationProvider;
    private OrgResolver orgResolver;

    /**
     * Provides a custom authentication handler that uses the configured Authenticator implementation.
     * This method initializes the authenticator from the provided configuration and returns this handler
     * instance configured with that authenticator.
     *
     * @param vertx       The Vertx instance
     * @param configObject  Configuration parameters containing authenticator provider class name and settings
     * @param orgResolver Organization resolver (not used in custom authentication)
     * @param meterRegistry for registering metrics
     * @return AuthenticationHandler
     * @throws InvalidConfigException if the authenticator provider class cannot be loaded or initialized
     */


    @Override
    public AuthenticationHandler provideHandler(
        Vertx vertx,
        JsonObject configObject,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    ) {
        return provideHandler(vertx, configObject.mapTo(AuthenticationOptions.class), orgResolver, meterRegistry);
    }

    private AuthenticationHandler provideHandler(
        Vertx vertx,
        AuthenticationOptions authenticationOptions,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    ) {
        try {
            if (StringUtils.isEmpty(authenticationOptions.getAuthenticatorClassName())) {
                throw new InvalidConfigException("Empty/Null Authenticator class name");
            }

            Class<?> providerClass = Class.forName(authenticationOptions.getAuthenticatorClassName());
            if (!AuthenticationProvider.class.isAssignableFrom(providerClass)) {
                throw new InvalidConfigException(
                    "Provider class " + providerClass.getName() + " does not implement Authenticator interface"
                );
            }
            authenticationProvider = (AuthenticationProvider)providerClass.getDeclaredConstructor().newInstance();

            try {
                authenticationProvider.init(authenticationOptions, meterRegistry);
            } catch (Exception e) {
                throw new InvalidConfigException("Failed to initialize authenticator: " + e.getMessage(), e);
            }

        } catch (ClassNotFoundException e) {
            throw new InvalidConfigException(
                "Authentication provider class not found: " + authenticationOptions.getAuthenticatorClassName(),
                e
            );
        } catch (ReflectiveOperationException e) {
            throw new InvalidConfigException("Failed to create authentication provider", e);
        }

        return new CustomAuthenticationHandler(authenticationProvider, orgResolver);
    }

    @Override
    public void handle(RoutingContext routingContext) {

        Org org = orgResolver.resolve(DEFAULT_ORG);
        RequestContext requestContext = null;
        try {
            requestContext = createRequestContext(routingContext);
        } catch (URISyntaxException e) {
            throw new BadRequestException(e);
        }

        Future<UserContext> userContext = authenticationProvider.authenticate(org.getName(), requestContext);

        userContext.onComplete(result -> {
            if (result.succeeded()) {
                routingContext.put(USER_CONTEXT, result.result());
                routingContext.next();
            } else {
                routingContext.fail(UNAUTHORIZED.code(), result.cause());
            }
        });
    }

    private RequestContext createRequestContext(RoutingContext routingContext) throws URISyntaxException {
        RequestContext httpContext = new RequestContext();
        httpContext.setUri(new URI(routingContext.request().uri()));

        httpContext.setHeaders(routingContext.request().headers());
        httpContext.setParams(routingContext.request().params());

        return httpContext;
    }
}
