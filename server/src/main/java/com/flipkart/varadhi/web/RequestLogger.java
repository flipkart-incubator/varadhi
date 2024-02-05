package com.flipkart.varadhi.web;

import com.flipkart.varadhi.core.entities.ApiContext;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.Constants.API_CONTEXT_KEY;

@Slf4j
public class RequestLogger  implements RouteConfigurator {

    private final SpanProvider spanProvider;

    public RequestLogger(SpanProvider spanProvider) {
        this.spanProvider = spanProvider;
    }

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(this::handle);
    }
    private void handle(RoutingContext ctx) {
        ApiContext apiContext = ctx.get(API_CONTEXT_KEY);
        if (null != apiContext) {
            ctx.response().endHandler((r) -> {
                apiContext.put(ApiContext.END_TIME, System.currentTimeMillis());
                String apiDetails = apiContext.toString();
                log.info(apiDetails);
                spanProvider.emitSpan("Request", apiDetails);
            });
        }
        ctx.next();
    }
}
