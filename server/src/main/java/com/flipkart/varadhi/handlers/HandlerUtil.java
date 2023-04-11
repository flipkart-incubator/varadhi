package com.flipkart.varadhi.handlers;

import com.flipkart.varadhi.RouteDefinition;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.stream.Collectors;

public class HandlerUtil {
    public static void handleTodo(RoutingContext context) {
        context.response().setStatusCode(500).setStatusMessage("Not Implemented").end();
    }

    public static List<RouteDefinition> withBasePath(String basePath, List<RouteDefinition> defs) {
        return defs.stream().map(d -> d.withPath(basePath + d.path())).collect(Collectors.toList());
    }
}
