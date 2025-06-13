package com.flipkart.varadhi.web.routes;

import io.vertx.ext.web.Route;

public interface RouteConfigurator {
    void configure(Route route, RouteDefinition routeDef);
}
