package com.flipkart.varadhi.web.routes;


import com.flipkart.varadhi.auth.PermissionAuthorization;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.Set;


/**
 * Route Definitions describes a route and associated behaviours. The setup code can use appropriate handlers based on
 * this definition.
 */

@Slf4j
public record RouteDefinition(HttpMethod method, String path, Set<RouteBehaviour> behaviours,
                              Handler<RoutingContext> handler,
                              Optional<PermissionAuthorization> requiredAuthorization) {

}