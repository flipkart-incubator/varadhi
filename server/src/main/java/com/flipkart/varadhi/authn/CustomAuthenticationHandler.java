package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.server.spi.RequestContext;
import com.flipkart.varadhi.server.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.server.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.server.spi.authn.AuthenticationProvider;
import com.flipkart.varadhi.server.spi.utils.OrgResolver;
import com.flipkart.varadhi.server.spi.vo.URLDefinition;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.InternalServerErrorException;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.common.Constants.ContextKeys.USER_CONTEXT;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_ORG;
import static com.flipkart.varadhi.server.spi.vo.Constants.DEFAULT_ORG;
import static com.flipkart.varadhi.server.spi.vo.URLDefinition.anyMatch;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@NoArgsConstructor
@Slf4j
public class CustomAuthenticationHandler implements AuthenticationHandler, AuthenticationHandlerProvider {
    private AuthenticationProvider authenticationProvider;

    private List<URLDefinition> orgExemptionURLs;


    public CustomAuthenticationHandler(
        AuthenticationProvider authenticationProvider,
        List<URLDefinition> orgExemptionURLs
    ) {
        this.authenticationProvider = authenticationProvider;
        this.orgExemptionURLs = orgExemptionURLs;
    }

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
            if (StringUtils.isEmpty(authenticationOptions.getAuthenticationProviderClassName())) {
                throw new InvalidConfigException("Empty/Null Authenticator class name");
            }

            Class<?> providerClass = Class.forName(authenticationOptions.getAuthenticationProviderClassName());
            if (!AuthenticationProvider.class.isAssignableFrom(providerClass)) {
                throw new InvalidConfigException(
                    "Provider class " + providerClass.getName() + " does not implement Authenticator interface"
                );
            }
            authenticationProvider = (AuthenticationProvider)providerClass.getDeclaredConstructor().newInstance();

            try {
                authenticationProvider.init(authenticationOptions, orgResolver, meterRegistry);
            } catch (Exception e) {
                throw new InvalidConfigException("Failed to initialize authenticator: " + e.getMessage(), e);
            }

        } catch (ClassNotFoundException e) {
            throw new InvalidConfigException(
                "Authentication provider class not found: " + authenticationOptions
                                                                                   .getAuthenticationProviderClassName(),
                e
            );
        } catch (ReflectiveOperationException e) {
            throw new InvalidConfigException("Failed to create authentication provider", e);
        }

        return new CustomAuthenticationHandler(
            authenticationProvider,
            authenticationOptions.getOrgContextExemptionURLs()
        );
    }

    @Override
    public void handle(RoutingContext routingContext) {

        RequestContext requestContext;
        try {
            requestContext = createRequestContext(routingContext);
        } catch (URISyntaxException e) {
            throw new BadRequestException(e);
        }

        String orgName = readOrgNameFromContext(routingContext);

        Future<UserContext> userContext = authenticationProvider.authenticate(orgName, requestContext);

        userContext.onComplete(result -> {
            if (result.succeeded()) {
                routingContext.put(USER_CONTEXT, result.result());
                routingContext.next();
            } else {
                routingContext.fail(UNAUTHORIZED.code(), result.cause());
            }
        });
    }

    private String readOrgNameFromContext(RoutingContext routingContext) {

        Map<ResourceType, ResourceHierarchy> typeHierarchyMap = routingContext.get(CONTEXT_KEY_RESOURCE_HIERARCHY);
        if (typeHierarchyMap != null) {
            if (typeHierarchyMap.containsKey(ResourceType.ORG)) {
                ResourceHierarchy hierarchy = typeHierarchyMap.get(ResourceType.ORG);
                if (hierarchy != null && hierarchy.getAttributes() != null && hierarchy.getAttributes()
                                                                                       .containsKey(TAG_ORG)) {
                    return hierarchy.getAttributes().get(TAG_ORG);
                }
            }
        }

        if (!anyMatch(
            this.orgExemptionURLs,
            String.valueOf(routingContext.request().method()),
            routingContext.request().path()
        )) {
            throw new InternalServerErrorException("Org context missing in the request");
        }

        return DEFAULT_ORG;
    }

    private RequestContext createRequestContext(RoutingContext routingContext) throws URISyntaxException {
        RequestContext httpContext = new RequestContext();
        httpContext.setUri(new URI(routingContext.request().uri()));

        httpContext.setHeaders(routingContext.request().headers());
        httpContext.setParams(routingContext.request().params());

        return httpContext;
    }
}
