package com.flipkart.varadhi.web.core;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class RequestBodyHandler implements Handler<RoutingContext> {
    // To work around the Vertx Handler priority/weight issue, as Varadhi needs to control the
    // sequence of handlers for each API.
    // Check Vertx RouteState to see the handler priority/weight.
    // This just wraps the Vertx BodyHandler and sets the body limit.
    private final BodyHandler bodyHandler;

    public RequestBodyHandler(int maxPayloadSize) {
        this.bodyHandler = BodyHandler.create(false);
        bodyHandler.setBodyLimit(maxPayloadSize);
    }

    @Override
    public void handle(RoutingContext ctx) {
        bodyHandler.handle(ctx);
    }
}
