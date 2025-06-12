package com.flipkart.varadhi.web.v1;


import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.common.exceptions.ServerNotAvailableException;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.hierarchy.Hierarchies;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.MethodNames.GET;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

@ExtensionMethod ({RoutingContextExtension.class})
public class HealthCheckHandler implements Handler<RoutingContext>, RouteProvider {

    private static final String API_NAME = "HEALTH_CHECK";

    // TODO: add appropriate checks

    private volatile int responseCode = HTTP_OK;
    private volatile String responseMsg = "iam_ok";

    @Override
    public void handle(RoutingContext ctx) {
        if (responseCode == HTTP_OK) {
            ctx.endApiWithResponse(responseMsg);
        } else {
            throw new ServerNotAvailableException(responseMsg);
        }
    }

    public void bringOOR() {
        responseCode = HTTP_UNAVAILABLE;
        responseMsg = "not_ok: under orr";
    }

    @Override
    public List<RouteDefinition> get() {
        return List.of(
            RouteDefinition.get(GET, API_NAME, "/v1/health-check")
                           .unAuthenticated()
                           .logsDisabled()
                           .tracingDisabled()
                           .build(this::getHierarchies, this)

        );
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        return Map.of(ResourceType.ROOT, new Hierarchies.RootHierarchy());
    }
}
