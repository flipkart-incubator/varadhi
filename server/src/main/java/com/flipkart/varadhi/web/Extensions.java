package com.flipkart.varadhi.web;

import com.flipkart.varadhi.utils.JsonMapper;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

public class Extensions {

    public static class RequestBodyExtension {

        /*
        Extension method for vertx RequestBody.
        builtin asPojo() method is not working because of jackson issues i.e.
        it needs default constructor and none final fields.

        Extending RequestBody to have asPojo() custom deserializer to convert requestBody to appropriate Pojo.
         */
        public static <T> T asPojo(RequestBody body, Class<T> clazz) {
            return JsonMapper.jsonDeserialize(body.asString(), clazz);
        }
    }

    @Slf4j
    public static class RoutingContextExtension {
        public static <T> void endRequestWithResponse(RoutingContext ctx, T response) {
            addResponseHeaders(ctx, true);
            String responseBody = JsonMapper.jsonSerialize(response);
            ctx.response().end(responseBody, (r) -> {
                HttpServerRequest request = ctx.request();
                if (r.succeeded()) {
                    log.debug("Request {}:{} completed successfully.", request.method(), request.path());
                } else {
                    log.error("Request {}:{} Failed to send response: {}", request.method(), request.path(), r.cause());
                }
            });
        }

        public static <T> void endRequest(RoutingContext ctx) {
            addResponseHeaders(ctx, false);
            ctx.response().end((r) -> {
                HttpServerRequest request = ctx.request();
                if (r.succeeded()) {
                    log.debug("Request {}:{} completed successfully.", request.method(), request.path());
                } else {
                    log.error("Request {}:{} Failed to complete:{}.", request.method(), request.path(), r.cause());
                }
            });
        }

        private static void addResponseHeaders(RoutingContext ctx, boolean hasContent) {
            if (hasContent) {
                ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
                ctx.response().putHeader(HttpHeaders.CONTENT_ENCODING, "utf-8");
            }
        }

        public static <T> void endRequestWithResponse(RoutingContext ctx, int status, T response) {
            ctx.response().setStatusCode(status);
            endRequestWithResponse(ctx, response);
        }

        public static void todo(RoutingContext context) {
            context.response().setStatusCode(500).setStatusMessage("Not Implemented").end();
        }
    }
}
