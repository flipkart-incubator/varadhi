package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.common.exceptions.InvalidConfigException;
import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.UserContext;
import com.flipkart.varadhi.server.spi.RequestContext;
import com.flipkart.varadhi.server.spi.authn.AuthenticationProvider;
import com.flipkart.varadhi.server.spi.utils.OrgResolver;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import jakarta.ws.rs.BadRequestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CustomAuthenticationHandlerTest {

    private CustomAuthenticationHandler handlerProvider;
    private Vertx vertx;
    private JsonObject jsonObject;
    private OrgResolver orgResolver;
    private MeterRegistry meterRegistry;
    private RoutingContext routingContext;
    private AuthenticationProvider authenticator;

    @BeforeEach
    void setUp() {
        handlerProvider = new CustomAuthenticationHandler();
        vertx = Vertx.vertx();
        jsonObject = new JsonObject();
        orgResolver = mock(OrgResolver.class);
        meterRegistry = mock(MeterRegistry.class);
        routingContext = mock(RoutingContext.class);
        authenticator = mock(AuthenticationProvider.class);
    }

    @Test
    void providesHandlerInitializesAuthenticator() {
        jsonObject.put("authenticationProviderClassName", "com.flipkart.varadhi.MockAuthenticator");
        jsonObject.put("orgContextExemptionURLs", new ArrayList<>());
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        assertNotNull(handler);
        assertInstanceOf(CustomAuthenticationHandler.class, handler);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void providesHandlerFailed(String value) {
        jsonObject.put("authenticationProviderClassName", value);
        assertThrows(
            InvalidConfigException.class,
            () -> handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry)
        );
    }

    @ParameterizedTest
    @ValueSource (strings = {"randomString", "com.flipkart.varadhi.RandomClass"})
    void providesHandlerFailedInvalidClassName(String value) {
        jsonObject.put("authenticationProviderClassName", value);
        assertThrows(
            InvalidConfigException.class,
            () -> handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry)
        );
    }

    @Test
    void handleAuthenticatesSuccessfully() {
        CustomAuthenticationHandler handler = new CustomAuthenticationHandler(authenticator);
        Org org = Org.of("testOrg");
        UserContext userContext = new UserContext() {
            @Override
            public String getSubject() {
                return "test:subject";
            }

            @Override
            public boolean isExpired() {
                return false;
            }
        };

        when(orgResolver.resolve(anyString())).thenReturn(org);
        when(routingContext.request()).thenReturn(mock(io.vertx.core.http.HttpServerRequest.class));
        when(routingContext.request().uri()).thenReturn("/test");
        when(authenticator.authenticate(anyString(), any(RequestContext.class))).thenReturn(
            Future.succeededFuture(userContext)
        );

        Map<ResourceType, ResourceHierarchy> typeHierarchyMap = new HashMap<>();
        typeHierarchyMap.put(ResourceType.ORG, new Hierarchies.OrgHierarchy("tmp"));

        when(routingContext.get(CONTEXT_KEY_RESOURCE_HIERARCHY)).thenReturn(typeHierarchyMap);

        handler.handle(routingContext);

        verify(routingContext).put(eq("userContext"), any(UserContext.class));
        verify(routingContext).next();
    }

    @Test
    void handleFailsAuthentication() {
        CustomAuthenticationHandler handler = new CustomAuthenticationHandler(authenticator);
        Org org = Org.of("testOrg");
        when(orgResolver.resolve(anyString())).thenReturn(org);
        when(routingContext.request()).thenReturn(mock(io.vertx.core.http.HttpServerRequest.class));
        when(routingContext.request().uri()).thenReturn("/test");
        when(authenticator.authenticate(anyString(), any(RequestContext.class))).thenReturn(
            Future.failedFuture(new Exception("Authentication failed"))
        );

        Map<ResourceType, ResourceHierarchy> typeHierarchyMap = new HashMap<>();
        typeHierarchyMap.put(ResourceType.ORG, new Hierarchies.OrgHierarchy("tmp"));

        when(routingContext.get(CONTEXT_KEY_RESOURCE_HIERARCHY)).thenReturn(typeHierarchyMap);

        handler.handle(routingContext);

        verify(routingContext).fail(eq(UNAUTHORIZED.code()), any(Throwable.class));
    }

    @Test
    void handleThrowsBadRequestException() throws URISyntaxException {
        CustomAuthenticationHandler handler = new CustomAuthenticationHandler(authenticator);
        when(routingContext.request()).thenReturn(mock(io.vertx.core.http.HttpServerRequest.class));
        when(routingContext.request().uri()).thenReturn("invalid_uri");
        when(orgResolver.resolve(anyString())).thenReturn(Org.of("testOrg"));
        when(authenticator.authenticate(anyString(), any(RequestContext.class))).thenThrow(BadRequestException.class);

        Map<ResourceType, ResourceHierarchy> typeHierarchyMap = new HashMap<>();
        typeHierarchyMap.put(ResourceType.ORG, new Hierarchies.OrgHierarchy("tmp"));

        when(routingContext.get(CONTEXT_KEY_RESOURCE_HIERARCHY)).thenReturn(typeHierarchyMap);

        assertThrows(BadRequestException.class, () -> handler.handle(routingContext));
    }
}
