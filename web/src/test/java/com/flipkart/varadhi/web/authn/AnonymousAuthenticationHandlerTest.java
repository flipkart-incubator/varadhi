package com.flipkart.varadhi.web.authn;

import com.flipkart.varadhi.web.spi.utils.OrgResolver;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.impl.SocketAddressImpl;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.SimpleAuthenticationHandler;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;

import static com.flipkart.varadhi.web.Extensions.ANONYMOUS_IDENTITY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith (MockitoExtension.class)
class AnonymousAuthenticationHandlerTest {

    private AnonymousAuthenticationHandler handlerProvider;
    private Vertx vertx;
    private JsonObject configObject;
    private OrgResolver orgResolver;
    private MeterRegistry meterRegistry;
    private RoutingContext routingContext;
    private HttpServerRequest request;
    private HttpServerResponse response;

    @BeforeEach
    void setUp() {
        handlerProvider = new AnonymousAuthenticationHandler();
        vertx = Vertx.vertx();
        configObject = new JsonObject();
        orgResolver = mock(OrgResolver.class);
        meterRegistry = mock(MeterRegistry.class);
        routingContext = mock(RoutingContext.class);
        request = mock(HttpServerRequest.class);
        response = mock(HttpServerResponse.class);
    }

    @Test
    void providesHandlerAllowsAnonymousAccess() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, configObject, orgResolver, meterRegistry);
        assertNotNull(handler);
        assertInstanceOf(SimpleAuthenticationHandler.class, handler);
    }

    @Test
    void anonymousAccessSucceeds() {
        AuthenticationHandler handler = handlerProvider.provideHandler(vertx, configObject, orgResolver, meterRegistry);

        doReturn(request).when(routingContext).request();
        SocketAddress socketAddress = new SocketAddressImpl(new InetSocketAddress("localhost", 8080));
        doReturn(socketAddress).when(request).remoteAddress();
        doReturn(null).when(routingContext).user();

        handler.handle(routingContext);

        ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);

        verify(routingContext).setUser(userCaptor.capture());

        User user = userCaptor.getValue();
        assertNotNull(user);
        Assertions.assertEquals(Extensions.ANONYMOUS_IDENTITY, user.subject());
        assertFalse(user.expired());
    }
}
