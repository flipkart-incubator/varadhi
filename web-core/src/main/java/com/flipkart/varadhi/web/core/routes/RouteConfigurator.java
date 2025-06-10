package com.flipkart.varadhi.web.core.routes;

import io.vertx.ext.web.Route;

public interface RouteConfigurator {
    void configure(Route route, RouteDefinition routeDef);
}
