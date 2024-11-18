package com.flipkart.varadhi.web;

import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;
import lombok.experimental.ExtensionMethod;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_IS_SUPER_USER;


@ExtensionMethod({Extensions.RoutingContextExtension.class})
public class SuperUserHandler implements RouteConfigurator {

    private final List<String> superUsers = new ArrayList<>();
    private final boolean isAuthorizationEnabled;

    public SuperUserHandler(AppConfiguration appConfiguration) {
        isAuthorizationEnabled = appConfiguration.isAuthorizationEnabled();
        if (isAuthorizationEnabled) {
            superUsers.addAll(appConfiguration.getAuthorization().getSuperUsers());
        }
    }

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(ctx -> {
            if (isAuthorizationEnabled) {
                String user = ctx.getIdentityOrDefault();
                ctx.put(CONTEXT_KEY_IS_SUPER_USER, superUsers.contains(user));
            }
            ctx.next();
        });
    }
}
