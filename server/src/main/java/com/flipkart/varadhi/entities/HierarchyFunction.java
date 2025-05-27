package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.EntityType;
import io.vertx.ext.web.RoutingContext;

import java.util.Map;

@FunctionalInterface
public interface HierarchyFunction {
    Map<EntityType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean parsedBody);
}
