package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.BaseResource;
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
        public static <T extends BaseResource> T asPojo(RequestBody body, Class<T> clazz) {
            T deserialzedObject = JsonMapper.jsonDeserialize(body.asString(), clazz);
            deserialzedObject.validate();
            return deserialzedObject;
        }
    }

    @Slf4j
    public static class RoutingContextExtension {
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

        public static void todo(RoutingContext context) {
            context.response().setStatusCode(500).setStatusMessage("Not Implemented").end();
        }
    }
}
