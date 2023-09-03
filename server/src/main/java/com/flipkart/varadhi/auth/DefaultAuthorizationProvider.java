package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.entities.UserContext;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class DefaultAuthorizationProvider implements AuthorizationProvider {

    private DefaultAuthorizationConfiguration configuration;

    @Override
    public Future<Boolean> init(JsonObject configuration) {
        this.configuration = configuration.mapTo(DefaultAuthorizationConfiguration.class);
        return Future.succeededFuture(true);
    }

    /**
     * Checks if the user {@code UserContext} is authorized to perform action {@code ResourceAction} on resource path.
     * @param userContext information on the user, contains the user identifier
     * @param action the action being performed by the user which is to be authorized
     * @param resource the path of the resource on which the action is to be authorized
     * @return {@code Future<Boolean>} a future result expressing True/False decision
     */
    @Override
    public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {

        return Future.succeededFuture(false);
    }

}
