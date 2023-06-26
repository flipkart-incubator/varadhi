package com.flipkart.varadhi.utils;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ResponseExtension {
    public static <T> void endRequestWithResponse(RoutingContext ctx, T response) {
        String responseBody = JsonMapper.jsonSerialize(response);
        ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        ctx.response().putHeader(HttpHeaders.CONTENT_ENCODING, "utf-8");
        ctx.response().end(responseBody, (r) -> {
            HttpServerRequest request = ctx.request();
            if (r.succeeded()) {
                log.debug("Request {}:{} completed successfully.", request.method(), request.path());
            } else {
                log.error("Request {}:{} Failed to send response: {}", request.method(), request.path(), r.cause());
            }
        });
    }

    public static <T> void endRequestWithResponse(RoutingContext ctx, int status, T response) {
        ctx.response().setStatusCode(status);
        endRequestWithResponse(ctx, response);
    }
}
