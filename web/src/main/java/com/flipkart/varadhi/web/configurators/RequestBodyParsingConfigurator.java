package com.flipkart.varadhi.web.configurators;

import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.vertx.ext.web.Route;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
public class RequestBodyParsingConfigurator implements RouteConfigurator {

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(ctx -> {
            routeDef.getBodyParser().accept(ctx);
            ctx.next();
        });
    }


}
