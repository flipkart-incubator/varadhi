package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.spi.RequestContext;
import com.flipkart.varadhi.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.spi.authn.Authenticator;
import com.flipkart.varadhi.spi.utils.OrgResolver;
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

import java.net.URI;
import java.net.URISyntaxException;

import static com.flipkart.varadhi.common.Constants.ContextKeys.USER_CONTEXT;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class CustomAuthenticationHandler implements AuthenticationHandler, AuthenticationHandlerProvider {
    private static final String DEFAULT_ORG = "SYSTEM";
    private Authenticator authenticator;
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
        return provideHandler(vertx, configObject.mapTo(AuthenticationConfig.class), orgResolver, meterRegistry);
    }

    private AuthenticationHandler provideHandler(
        Vertx vertx,
        AuthenticationConfig authenticationConfig,
        OrgResolver orgResolver,
        MeterRegistry meterRegistry
    ) {
        try {
            if (authenticationConfig.getAuthenticatorClassName().isEmpty()) {
                throw new InvalidConfigException("Empty/Null Authenticator class name");
            }

            Class<?> providerClass = Class.forName(authenticationConfig.getAuthenticatorClassName());
            if (!Authenticator.class.isAssignableFrom(providerClass)) {
                throw new InvalidConfigException(
                    "Provider class " + providerClass.getName() + " does not implement Authenticator interface"
                );
            }
            authenticator = (Authenticator)providerClass.getDeclaredConstructor().newInstance();

            try {
                authenticator.init(authenticationConfig, meterRegistry);
            } catch (Exception e) {
                throw new InvalidConfigException("Failed to initialize authenticator: " + e.getMessage(), e);
            }

        } catch (ClassNotFoundException e) {
            throw new InvalidConfigException(
                "Authentication provider class not found: " + authenticationConfig.getAuthenticatorClassName(),
                e
            );
        } catch (ReflectiveOperationException e) {
            throw new InvalidConfigException("Failed to create authentication provider", e);
        }

        return new CustomAuthenticationHandler(authenticator, orgResolver);
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

        Future<UserContext> userContext = authenticator.authenticate(org.getName(), requestContext);

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
