package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.entities.utils.RequestContext;
import com.flipkart.varadhi.spi.authn.AuthenticationProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
public class CustomAuthenticationHandler implements AuthenticationHandler {
    AuthenticationProvider authenticationProvider;

    public AuthenticationHandler provideHandler(Vertx vertx, AuthenticationConfig authenticationConfig) {
        try {
            authenticationProvider = (AuthenticationProvider)Class.forName(authenticationConfig.getProviderClassName())
                                                                  .getDeclaredConstructor()
                                                                  .newInstance();
            authenticationProvider.init(authenticationConfig);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create authenticationConfig provider", e);
        }
        return new CustomAuthenticationHandler(authenticationProvider);
    }

    @Override
    public void handle(RoutingContext routingContext) {
        Org org = (Org)routingContext.get("org");

        Future<UserContext> userContext = authenticationProvider.authenticate(
            org,
            createRequestContext(routingContext)
        );

        userContext.onComplete(result -> {
            if (result.succeeded()) {
                routingContext.put("userContext", result.result());
                routingContext.next();
            } else {
                routingContext.fail(401);
            }
        });
    }

    RequestContext createRequestContext(RoutingContext routingContext) {
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
