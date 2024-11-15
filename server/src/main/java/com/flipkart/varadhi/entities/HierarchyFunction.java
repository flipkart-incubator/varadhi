package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.ResourceType;
import io.vertx.ext.web.RoutingContext;

import java.util.Map;

@FunctionalInterface
public interface HierarchyFunction {
    Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean parsedBody);
}
