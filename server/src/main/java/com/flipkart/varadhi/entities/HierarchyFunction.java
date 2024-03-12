package com.flipkart.varadhi.entities;

import io.vertx.ext.web.RoutingContext;

@FunctionalInterface
public interface HierarchyFunction {
    ResourceHierarchy getHierarchy(RoutingContext ctx, boolean parsedBody);
}
