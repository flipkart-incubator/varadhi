package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteBehaviour;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;


@Slf4j
public class HierarchyHandler implements RouteConfigurator {
    public HierarchyHandler() {
    }

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(ctx -> {
            boolean hasParsedBody = routeDef.getBehaviours().contains(RouteBehaviour.parseBody);
            Map<ResourceType, ResourceHierarchy> hierarchies = routeDef.getHierarchyFunction()
                                                                       .getHierarchies(ctx, hasParsedBody);
            ctx.put(CONTEXT_KEY_RESOURCE_HIERARCHY, hierarchies);
            ctx.next();
        });
    }
}
