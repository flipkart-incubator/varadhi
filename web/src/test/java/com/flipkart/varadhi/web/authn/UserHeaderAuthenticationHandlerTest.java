package com.flipkart.varadhi.web.authn;

import com.flipkart.varadhi.web.spi.utils.OrgResolver;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static com.flipkart.varadhi.common.Constants.USER_ID_HEADER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class UserHeaderAuthenticationHandlerTest {

    private UserHeaderAuthenticationHandler handlerProvider;
    private Vertx vertx;
    private JsonObject jsonObject;
    private OrgResolver orgResolver;
    private MeterRegistry meterRegistry;
    private RoutingContext routingContext;
    private HttpServerRequest request;
    private MultiMap headers = new HeadersMultiMap();
    private HttpServerResponse response;

    @BeforeEach
    void setUp() {
        handlerProvider = new UserHeaderAuthenticationHandler();
        vertx = Vertx.vertx();
        jsonObject = new JsonObject();
        orgResolver = mock(OrgResolver.class);
        meterRegistry = mock(MeterRegistry.class);
        routingContext = mock(RoutingContext.class);
        request = mock(HttpServerRequest.class);
        doReturn("/hello/world").when(request).uri();
        doReturn(new HeadersMultiMap()).when(request).params();
        doReturn(headers).when(request).headers();
        response = mock(HttpServerResponse.class);
    }

    @Test
    void providesHandlerAllowsUserHeaderAccess() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        assertNotNull(handler);
        assertTrue(handler instanceof SimpleAuthenticationHandler);
    }

    @Test
    void userHeaderAccessSucceeds() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        when(routingContext.request()).thenReturn(request);
        headers.add(USER_ID_HEADER, "testUser");

        handler.handle(routingContext);

        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
        verify(routingContext).setUser(userCaptor.capture());

        User user = userCaptor.getValue();
        assertNotNull(user);
        assertEquals("testUser", user.subject());
        assertFalse(user.expired());
    }

    @Test
    void userHeaderAccessFailsWhenHeaderIsEmpty() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        when(routingContext.request()).thenReturn(request);
        headers.add(USER_ID_HEADER, "");
        handler.handle(routingContext);

        verify(routingContext, never()).setUser(any());
        verify(routingContext).fail(eq(401), any(HttpException.class));
    }

    @Test
    void userHeaderAccessFailsWhenHeaderIsMissing() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        when(routingContext.request()).thenReturn(request);
        handler.handle(routingContext);

        verify(routingContext, never()).setUser(any());
        verify(routingContext).fail(eq(401), any(HttpException.class));
    }
}
