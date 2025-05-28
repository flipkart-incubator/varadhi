package com.flipkart.varadhi.entities;

import io.vertx.ext.web.RoutingContext;

import java.util.Map;

@FunctionalInterface
public interface HierarchyFunction {
    Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean parsedBody);
}
