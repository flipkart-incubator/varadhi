package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;

public class HierarchyHandler implements RouteConfigurator {
    public HierarchyHandler() {
    }

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(ctx -> {
            boolean hasParsedBody = routeDef.getBehaviours().contains(RouteBehaviour.parseBody);
            ResourceHierarchy hierarchy = routeDef.getHierarchyFunction().getHierarchy(ctx, hasParsedBody);
            ctx.put(CONTEXT_KEY_RESOURCE_HIERARCHY, hierarchy);
            ctx.next();
        });
    }
}
