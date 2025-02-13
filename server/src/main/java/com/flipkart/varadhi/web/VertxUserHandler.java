package com.flipkart.varadhi.web;

import com.flipkart.varadhi.authn.AuthenticationMechanism;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.entities.auth.UserContext;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;
import java.util.Map;

public class VertxUserHandler implements Handler<RoutingContext> {

    private AuthenticationMechanism authenticationMechanism;

    public VertxUserHandler(Vertx vertx, AppConfiguration configuration) {
        authenticationMechanism = configuration.getAuthentication().getMechanism();
    }

    @Override
    public void handle(RoutingContext routingContext) {

        if (authenticationMechanism != AuthenticationMechanism.custom) {
            User user = routingContext.user();
            if (user != null) {
                routingContext.put("userContext", new UserContext() {
                    @Override
                    public String getSubject() {
                        return user.subject();
                    }

                    @Override
                    public boolean isExpired() {
                        return user.expired();
                    }

                    @Override
                    public Map<String, String> getAttributes() {
                        return mapFromJsonObject(user.attributes());
                    }
                });
            } else {
                routingContext.fail(401);
            }
        }
        routingContext.next();
    }

    protected Map<String, String> mapFromJsonObject(JsonObject jsonObject) {
        Map<String, String> map = new HashMap<>();
        if (jsonObject != null) {
            jsonObject.forEach(entry -> map.put(entry.getKey(), entry.getValue().toString()));
        }
        return map;
    }
}
