package com.flipkart.varadhi.web.v1;

import com.flipkart.varadhi.utils.RequestBodyExtension;
import com.flipkart.varadhi.utils.ResponseExtension;
import com.flipkart.varadhi.web.RouteDefinition;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

@ExtensionMethod({ResponseExtension.class})
public class HealthCheckHandler implements Handler<RoutingContext>, RouteDefinition.Provider {

    // TODO: add appropriate checks

    private volatile int responseCode = HTTP_OK;
    private volatile String responseMsg = "iam_ok";

    @Override
    public void handle(RoutingContext ctx) {
        ctx.endRequestWithResponse(responseMsg);

    }

    public void bringOOR() {
        responseCode = HTTP_UNAVAILABLE;
        responseMsg = "not_ok: under orr";
    }

    @Override
    public List<RouteDefinition> get() {
        return List.of(
                new RouteDefinition(HttpMethod.GET, "/v1/health-check", Set.of(), this, Optional.empty())
        );
    }
}
