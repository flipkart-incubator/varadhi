package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.spi.utils.OrgResolver;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.common.Constants.USER_ID_HEADER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UserHeaderAuthenticationHandlerTest {

    private UserHeaderAuthenticationHandler handlerProvider;
    private Vertx vertx;
    private JsonObject jsonObject;
    private OrgResolver orgResolver;
    private MeterRegistry meterRegistry;
    private RoutingContext routingContext;

    @BeforeEach
    void setUp() {
        handlerProvider = new UserHeaderAuthenticationHandler();
        vertx = Vertx.vertx();
        jsonObject = new JsonObject();
        orgResolver = mock(OrgResolver.class);
        meterRegistry = mock(MeterRegistry.class);
        routingContext = mock(RoutingContext.class);
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
        when(routingContext.request()).thenReturn(mock(io.vertx.core.http.HttpServerRequest.class));
        when(routingContext.request().getHeader(USER_ID_HEADER)).thenReturn("testUser");

        Promise<User> promise = Promise.promise();
        handler.handle(routingContext);

        promise.future().onComplete(ar -> {
            if (ar.succeeded()) {
                User user = ar.result();
                assertNotNull(user);
                assertEquals("testUser", user.principal().getString("username"));
            } else {
                fail(ar.cause().getMessage());
            }
        });
    }

    @Test
    void userHeaderAccessFailsWhenHeaderIsMissing() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        when(routingContext.request()).thenReturn(mock(io.vertx.core.http.HttpServerRequest.class));
        when(routingContext.request().getHeader(USER_ID_HEADER)).thenReturn(null);

        Promise<User> promise = Promise.promise();
        handler.handle(routingContext);

        promise.future().onComplete(ar -> {
            assertFalse(ar.succeeded());
            assertEquals("no user details present", ar.cause().getMessage());
        });
    }
}
