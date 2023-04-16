package com.flipkart.varadhi.handlers;

import io.vertx.ext.web.RoutingContext;

public class HandlerUtil {
    public static void handleTodo(RoutingContext context) {
        context.response().setStatusCode(500).setStatusMessage("Not Implemented").end();
    }
}
