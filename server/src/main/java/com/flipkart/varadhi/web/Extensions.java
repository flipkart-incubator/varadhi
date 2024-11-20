package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.Validatable;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.utils.AsyncProducerReadStream;
import com.flipkart.varadhi.utils.JsonMapper;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;


import static com.flipkart.varadhi.Constants.CONTEXT_KEY_IS_SUPER_USER;
import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_IDENTITY;


public class Extensions {

    public static class RequestBodyExtension {

        /*
        Extension method for vertx RequestBody.
        builtin asPojo() method is not working because of jackson issues i.e.
        it needs default constructor and none final fields.

        Extending RequestBody to have asPojo() custom deserializer to convert requestBody to appropriate Pojo.
         */
        public static <T extends Validatable> T asValidatedPojo(RequestBody body, Class<T> clazz) {
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
        public static <T> void handleResponse(RoutingContext ctx, CompletableFuture<T> result) {
            result.whenComplete((t, error) -> ctx.vertx().runOnContext((Void) -> {
                if (error != null) {
                    endRequestWithException(ctx, unwrapExecutionException(error));
                } else {
                    if (null == t) {
                        endRequest(ctx);
                    } else {
                        endRequestWithResponse(ctx, t);
                    }
                }
            }));
        }

        public static <T> void handleChunkedResponse(
                RoutingContext ctx, Function<Consumer<T>, CompletableFuture<Void>> executor
        ) {

            AsyncProducerReadStream readStream = new AsyncProducerReadStream();
            ctx.response().setChunked(true);
            addResponseHeaders(ctx, true);
            ctx.response().send(readStream);
            executor.apply(m -> {
                        String responseBody = JsonMapper.jsonSerialize(m);
                        readStream.send(responseBody);
                    })
                    .whenComplete((t, error) -> ctx.vertx().runOnContext((Void) -> {
                        if (error != null) {
                            log.error("Completing chunked request response failure: {}", error.getMessage());
                            readStream.end(error);
                        } else {
                            log.info("Completing chunked request response");
                            readStream.end();
                        }
                    }));
        }

        private static Throwable unwrapExecutionException(Throwable t) {
            if (t instanceof ExecutionException) {
                return t.getCause();
            } else {
                return t;
            }
        }

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

        public static String getIdentityOrDefault(RoutingContext ctx) {
            User user = ctx.user();
            return null != user ? user.subject() : ANONYMOUS_IDENTITY;
        }

        // returns true if the request is from one of the configured super user(s).
        // This can be used to make certain actions conditional on super user in addition to regular authz checks.
        // e.g. allow subscriptions properties to be updated beyond configured permissible limits.
        public static boolean isSuperUser(RoutingContext ctx) {
            return ctx.get(CONTEXT_KEY_IS_SUPER_USER, false);
        }

        public static Map<String, String> getRequestAttributes(RoutingContext ctx) {
            Map<String, String> requestAttributes = new HashMap<>();
            Map<ResourceType, ResourceHierarchy> empty = Map.of();
            ctx.get(CONTEXT_KEY_RESOURCE_HIERARCHY, empty).values()
                    .forEach(h -> requestAttributes.putAll(h.getAttributes()));
            return requestAttributes;
        }
    }
}
