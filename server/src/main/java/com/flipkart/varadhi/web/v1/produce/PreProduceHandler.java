package com.flipkart.varadhi.web.v1.produce;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PreProduceHandler implements Handler<RoutingContext> {

    /**
     * Pre produce handler to do validations before producing it as message to data store.
     *
     * @param ctx the routing context to handle
     */
    @Override
    public void handle(RoutingContext ctx) {
        ctx.next();
    }
}
