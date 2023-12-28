package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.BaseResource;
import com.flipkart.varadhi.exceptions.NotImplementedException;
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
        public static <T extends BaseResource> T asValidatedPojo(RequestBody body, Class<T> clazz) {
            T deserialzedObject = asPojo(body, clazz);
            deserialzedObject.validate();
            return deserialzedObject;
        }

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


        public static void endRequestWithStatusAndErrorMsg(RoutingContext ctx, int httpStatus, String errorMessage) {
            addResponseHeaders(ctx, true);
            ctx.response().setStatusCode(httpStatus);
            ctx.response().setStatusMessage(errorMessage);
            endRequestWithResponse(ctx, new ErrorResponse(errorMessage));
        }

        public static void endRequest(RoutingContext ctx) {
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

        public static <T extends Throwable> void endRequestWithException(RoutingContext ctx, T throwable) {
            ctx.fail(throwable);
        }

        private static void addResponseHeaders(RoutingContext ctx, boolean hasContent) {
            if (hasContent) {
                ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
                ctx.response().putHeader(HttpHeaders.CONTENT_ENCODING, "utf-8");
            }
        }

        // Finish blocking handler by calling this for setting the API response.
        public static <T> void endApiWithResponse(RoutingContext ctx, T response) {
            ctx.put("api-response", response);
        }


        // Finish blocking handler by calling this for without setting API response.
        public static void endApi(RoutingContext ctx) {
            ctx.remove("api-response");
        }


        public static <T> T getApiResponse(RoutingContext ctx) {
            return ctx.get("api-response");
        }

        public static void todo(RoutingContext context) {
            throw new NotImplementedException("Not Implemented.");
        }
    }
}
