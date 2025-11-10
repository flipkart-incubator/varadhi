package com.flipkart.varadhi.web.authn;

import com.flipkart.varadhi.common.exceptions.ServerErrorException;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.spi.RequestContext;
import com.flipkart.varadhi.web.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.web.spi.authn.AuthenticationOptions;
import com.flipkart.varadhi.web.spi.authn.AuthenticationProvider;
import com.flipkart.varadhi.web.spi.utils.OrgResolver;
import com.flipkart.varadhi.web.spi.vo.URLDefinition;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import jakarta.ws.rs.BadRequestException;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.ContextKeys.RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_ORG;
import static com.flipkart.varadhi.web.spi.vo.URLDefinition.anyMatch;
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
                    "Provider class " + providerClass.getName() + " does not implement AuthenticationProvider interface"
                );
            }

            AuthenticationProvider authenticationProvider = (AuthenticationProvider)providerClass
                                                                                                 .getDeclaredConstructor()
                                                                                                 .newInstance();

            try {
                authenticationProvider.init(authenticationOptions, orgResolver, meterRegistry);
            } catch (Exception e) {
                throw new InvalidConfigException("Failed to initialize authenticator: " + e.getMessage(), e);
            }

            List<URLDefinition> exemptions = authenticationOptions.getOrgContextExemptionURLs() != null ?
                authenticationOptions.getOrgContextExemptionURLs() :
                Collections.emptyList();

            return new CustomAuthenticationHandler(authenticationProvider, exemptions);
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigException(
                "Authentication provider class not found: " + authenticationOptions
                                                                                   .getAuthenticationProviderClassName(),
                e
            );
        } catch (ReflectiveOperationException e) {
            throw new InvalidConfigException("Failed to create authentication provider", e);
        }
    }

    @Override
    public void handle(RoutingContext routingContext) {

        RequestContext requestContext;
        try {
            requestContext = new RequestContext(routingContext);
        } catch (URISyntaxException e) {
            throw new BadRequestException(e);
        }

        String orgName = readOrgNameFromContext(routingContext);

        // TODO: authenticationProvider only gives UserContext as the result, but that is not playing nice with the routingContext.user().
        // There is no way to convert UserContext to vertx User directly.
        Future<UserContext> userContext = authenticationProvider.authenticate(orgName, requestContext);

        userContext.onComplete(result -> {
            if (result.succeeded()) {
                routingContext.setUser(User.fromName(result.result().getSubject()));
                routingContext.next();
            } else {
                routingContext.fail(UNAUTHORIZED.code(), result.cause());
            }
        });
    }

    private String readOrgNameFromContext(RoutingContext routingContext) {

        String orgName = getOrgNameFromContext(routingContext.get(RESOURCE_HIERARCHY));

        if (!StringUtils.isBlank(orgName)) {
            return orgName;
        }

        if (!anyMatch(
            this.orgExemptionURLs,
            String.valueOf(routingContext.request().method()),
            routingContext.request().path()
        )) {
            throw new ServerErrorException("Org context missing in the request");
        }

        return orgName;
    }

    private String getOrgNameFromContext(Map<ResourceType, ResourceHierarchy> typeHierarchyMap) {
        if (typeHierarchyMap != null) {
            // since org is at the top of hierarchy, we can expect this info present in any entry present in the map
            for (ResourceHierarchy hierarchy : typeHierarchyMap.values()) {
                if (hierarchy.getAttributes() != null && hierarchy.getAttributes().containsKey(TAG_ORG)) {
                    return hierarchy.getAttributes().get(TAG_ORG);
                }
            }
        }
        return "";
    }
}
