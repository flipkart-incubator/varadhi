package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.entities.utils.RequestContext;
import com.flipkart.varadhi.spi.authn.AuthenticationHandlerProvider;
import com.flipkart.varadhi.spi.authn.Authenticator;
import com.flipkart.varadhi.spi.utils.OrgResolver;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import jakarta.ws.rs.BadRequestException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.Constants.ContextKeys.USER_CONTEXT;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

@AllArgsConstructor
@NoArgsConstructor
public class CustomAuthenticationHandler implements AuthenticationHandler, AuthenticationHandlerProvider {
    private Authenticator authenticator;

    /**
     * Provides a custom authentication handler that uses the configured Authenticator implementation.
     * This method initializes the authenticator from the provided configuration and returns this handler
     * instance configured with that authenticator.
     *
     * @param vertx The Vertx instance
     * @param jsonObject Configuration parameters containing authenticator provider class name and settings
     * @param orgResolver Organization resolver (not used in custom authentication)
     * @return This CustomAuthenticationHandler instance configured with the initialized authenticator
     * @throws RuntimeException if the authenticator provider class cannot be loaded or initialized
     */


    @Override
    public AuthenticationHandler provideHandler(Vertx vertx, JsonObject jsonObject, OrgResolver orgResolver) {
        return provideHandler(vertx, jsonObject.mapTo(AuthenticationConfig.class));
    }

    private AuthenticationHandler provideHandler(Vertx vertx, AuthenticationConfig authenticationConfig) {
        try {
            Class<?> providerClass = Class.forName(authenticationConfig.getProviderClassName());
            if (!Authenticator.class.isAssignableFrom(providerClass)) {
                throw new RuntimeException(
                    "Provider class " + providerClass.getName() + " does not implement AuthenticationProvider interface"
                );
            }
            authenticator = (Authenticator)providerClass.getDeclaredConstructor().newInstance();
            authenticator.init(authenticationConfig);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                "Authentication provider class not found: " + authenticationConfig.getProviderClassName(),
                e
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create authentication provider", e);
        }

        return new CustomAuthenticationHandler(authenticator);
    }

    private Org parseOrg(RoutingContext routingContext) {
        try {
            Map<ResourceType, ResourceHierarchy> hierarchies = routingContext.get(CONTEXT_KEY_RESOURCE_HIERARCHY);
            ResourceHierarchy hf = hierarchies.get(ResourceType.ORG);
            String orgName = hf.getResourcePath().substring(1);

            return Org.of(orgName);
        } catch (Exception e) {
            throw new BadRequestException("Invalid org");
        }
    }

    @Override
    public void handle(RoutingContext routingContext) {

        Org org = parseOrg(routingContext);

        Future<UserContext> userContext = authenticator.authenticate(
            org,
            createRequestContext(routingContext)
        );

        userContext.onComplete(result -> {
            if (result.succeeded()) {
                routingContext.put(USER_CONTEXT, result.result());
                routingContext.next();
            } else {
                routingContext.fail(UNAUTHORIZED.code(), result.cause());
            }
        });
    }

    private RequestContext createRequestContext(RoutingContext routingContext) {
        RequestContext httpContext = new RequestContext();
        try {
            httpContext.setUri(new URI(routingContext.request().uri()));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        httpContext.setHeaders(getAllHeaders(routingContext.request()));
        httpContext.setParams(getAllParams(routingContext.request()));

        return httpContext;
    }

    private Map<String, String> getAllHeaders(HttpServerRequest request) {
        Map<String, String> headers = new HashMap<>();
        request.headers().forEach(entry -> headers.put(entry.getKey().toLowerCase(), entry.getValue()));
        return headers;
    }

    private Map<String, String> getAllParams(HttpServerRequest request) {
        Map<String, String> params = new HashMap<>();
        request.params().forEach(entry -> params.put(entry.getKey(), entry.getValue()));
        return params;
    }


}
