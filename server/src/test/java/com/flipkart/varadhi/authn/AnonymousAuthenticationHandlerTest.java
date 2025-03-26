package com.flipkart.varadhi.authn;

import com.flipkart.varadhi.spi.utils.OrgResolver;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AnonymousAuthenticationHandlerTest {

    private AnonymousAuthenticationHandler handlerProvider;
    private Vertx vertx;
    private JsonObject jsonObject;
    private OrgResolver orgResolver;
    private MeterRegistry meterRegistry;
    private RoutingContext routingContext;

    @BeforeEach
    void setUp() {
        handlerProvider = new AnonymousAuthenticationHandler();
        vertx = Vertx.vertx();
        jsonObject = new JsonObject();
        orgResolver = mock(OrgResolver.class);
        meterRegistry = mock(MeterRegistry.class);
        routingContext = mock(RoutingContext.class);
    }

    @Test
    void providesHandlerAllowsAnonymousAccess() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        assertNotNull(handler);
        assertInstanceOf(SimpleAuthenticationHandler.class, handler);
    }

    @Test
    void anonymousAccessSucceeds() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, jsonObject, orgResolver, meterRegistry);
        when(routingContext.request()).thenReturn(mock(io.vertx.core.http.HttpServerRequest.class));
        when(routingContext.request().remoteAddress()).thenReturn(mock(io.vertx.core.net.SocketAddress.class));

        Promise<User> promise = Promise.promise();
        handler.handle(routingContext);

        promise.future().onComplete(ar -> {
            if (ar.succeeded()) {
                User user = ar.result();
                assertNotNull(user);
                assertEquals("anonymous", user.principal().getString("username"));
            } else {
                fail(ar.cause().getMessage());
            }
        });
    }
}
