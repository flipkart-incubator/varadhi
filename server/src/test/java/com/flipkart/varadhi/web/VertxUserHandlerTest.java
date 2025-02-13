package com.flipkart.varadhi.web;

import com.flipkart.varadhi.authn.AuthenticationMechanism;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.config.AuthenticationConfig;
import com.flipkart.varadhi.entities.auth.UserContext;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VertxUserHandlerTest {

    private VertxUserHandler vertxUserHandler;
    private RoutingContext routingContext;
    private User user;
    private AppConfiguration configuration;
    private Vertx vertx;
    private AuthenticationConfig authenticationConfig;


    @BeforeEach
    void setUp() {

        authenticationConfig = new AuthenticationConfig();
        authenticationConfig.setMechanism(AuthenticationMechanism.custom);

        vertx = Vertx.vertx();
        configuration = mock(AppConfiguration.class);

        when(configuration.getAuthentication()).thenReturn(authenticationConfig);

        vertxUserHandler = new VertxUserHandler(vertx, configuration);
        routingContext = spy(RoutingContext.class);
        user = mock(User.class);
    }

    @Test
    void handle_shouldAddUserContextToRoutingContext_whenUserIsAuthenticated() {
        authenticationConfig.setMechanism(AuthenticationMechanism.anonymous);

        vertxUserHandler = new VertxUserHandler(vertx, configuration);

        when(configuration.getAuthentication()).thenReturn(authenticationConfig);
        when(routingContext.user()).thenReturn(user);
        when(user.subject()).thenReturn("testSubject");
        when(user.expired()).thenReturn(false);
        when(user.attributes()).thenReturn(new JsonObject().put("key", "value"));

        ArgumentCaptor<UserContext> argumentCaptor = ArgumentCaptor.forClass(UserContext.class);

        vertxUserHandler.handle(routingContext);

        verify(routingContext).put(eq("userContext"), argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());

        assertEquals("testSubject", argumentCaptor.getValue().getSubject());
        assertEquals("value", argumentCaptor.getValue().getAttributes().get("key"));
        assertFalse(argumentCaptor.getValue().isExpired());
    }

    @Test
    void handle_shouldFailWith401_whenUserIsNotAuthenticated() {
        authenticationConfig.setMechanism(AuthenticationMechanism.anonymous);

        when(configuration.getAuthentication()).thenReturn(authenticationConfig);
        when(routingContext.user()).thenReturn(null);

        vertxUserHandler = new VertxUserHandler(vertx, configuration);

        vertxUserHandler.handle(routingContext);

        verify(routingContext).fail(401);
    }

    @Test
    void mapFromJsonObject_shouldReturnEmptyMap_whenJsonObjectIsNull() {
        when(configuration.getAuthentication()).thenReturn(authenticationConfig);
        Map<String, String> result = vertxUserHandler.mapFromJsonObject(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void mapFromJsonObject_shouldConvertJsonObjectToMap() {
        when(configuration.getAuthentication()).thenReturn(authenticationConfig);
        JsonObject jsonObject = new JsonObject().put("key1", "value1").put("key2", "value2");

        Map<String, String> result = vertxUserHandler.mapFromJsonObject(jsonObject);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
    }

    @Test
    void handle_custom_authentication_mechanism() {
        this.authenticationConfig.setMechanism(AuthenticationMechanism.custom);
        when(configuration.getAuthentication()).thenReturn(this.authenticationConfig);

        vertxUserHandler.handle(routingContext);
        verify(routingContext).next();

    }
}
