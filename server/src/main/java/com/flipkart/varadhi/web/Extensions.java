package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.utils.JsonSeqStream;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import com.flipkart.varadhi.common.Constants.*;

import static com.flipkart.varadhi.common.Constants.ContextKeys.RESOURCE_HIERARCHY;

public class Extensions {
    public static final String ANONYMOUS_IDENTITY = "Anonymous";

    private enum ContentKind {
        APPLICATION_JSON(HttpHeaderValues.APPLICATION_JSON.toString()), APPLICATION_JSON_SEQ(
            "application/json-seq"
        ), NONE("");

        private final String value;

        ContentKind(String value) {
            this.value = value;
        }
    }


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
                    ctx.fail(unwrapExecutionException(error));
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
            RoutingContext ctx,
            Function<Consumer<T>, CompletableFuture<Void>> executor
        ) {
            JsonSeqStream seqStream = new JsonSeqStream();
            ctx.response().setChunked(true);
            addResponseHeaders(ctx, ContentKind.APPLICATION_JSON_SEQ);
            ctx.response().send(seqStream);
            executor.apply(seqStream::send).whenComplete((t, error) -> ctx.vertx().runOnContext((Void) -> {
                if (error != null) {
                    log.error("Completing chunked request response failure: {}", error.getMessage());
                    seqStream.end(error);
                } else {
                    log.info("Completing chunked request response");
                    seqStream.end();
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
            endRequestWithResponse(ctx, response);

        }

        public static <T> void endRequestWithResponse(RoutingContext ctx, int code, T response) {
            addResponseHeaders(ctx, ContentKind.APPLICATION_JSON);
            ctx.response().setStatusCode(code);
            String responseBody = JsonMapper.jsonSerialize(response);
            ctx.response().end(responseBody, (r) -> {
                HttpServerRequest request = ctx.request();
                if (!r.succeeded()) {
                    log.error("Request {}:{} Failed to send response: {}", request.method(), request.path(), r.cause());
                }
            });
        }

        public static void endRequestWithStatusAndErrorMsg(RoutingContext ctx, int httpStatus, String errorMessage) {
            addResponseHeaders(ctx, ContentKind.APPLICATION_JSON);
            ctx.response().setStatusMessage(errorMessage);
            endRequestWithResponse(ctx, httpStatus, new ErrorResponse(errorMessage));
        }

        public static void endRequest(RoutingContext ctx) {
            addResponseHeaders(ctx, ContentKind.NONE);
            ctx.response().setStatusCode(HttpResponseStatus.NO_CONTENT.code());
            ctx.response().end((r) -> {
                HttpServerRequest request = ctx.request();
                if (!r.succeeded()) {
                    log.error("Request {}:{} Failed to complete:{}.", request.method(), request.path(), r.cause());
                }
            });
        }

        private static void addResponseHeaders(RoutingContext ctx, ContentKind kind) {
            if (kind != ContentKind.NONE) {
                ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, kind.value);
            }
        }

        // Finish blocking handler by calling this for setting the API response.
        public static <T> void endApiWithResponse(RoutingContext ctx, T response) {
            ctx.put(ContextKeys.API_RESPONSE, response);
        }

        // Finish blocking handler by calling this for without setting API response.
        public static void endApi(RoutingContext ctx) {
            ctx.remove(ContextKeys.API_RESPONSE);
        }

        public static <T> T getApiResponse(RoutingContext ctx) {
            return ctx.get(ContextKeys.API_RESPONSE);
        }

        public static void todo(RoutingContext context) {
            throw new UnsupportedOperationException("Not Implemented.");
        }

        public static String getIdentityOrDefault(RoutingContext ctx) {
            User user = ctx.user();
            return null != user ? user.subject() : ANONYMOUS_IDENTITY;
        }

        // returns true if the request is from one of the configured super user(s).
        // This can be used to make certain actions conditional on superuser in addition to regular authz checks.
        // e.g. allow subscriptions properties to be updated beyond configured permissible limits.
        public static boolean isSuperUser(RoutingContext ctx) {
            return ctx.get(ContextKeys.IS_SUPER_USER, false);
        }

        public static AttributesBuilder getRequestAttributes(RoutingContext ctx) {
            var requestAttributes = Attributes.builder();
            Map<ResourceType, ResourceHierarchy> resourceHierarchy = ctx.get(RESOURCE_HIERARCHY);
            if (resourceHierarchy != null) {
                resourceHierarchy.values().forEach(h -> h.getAttributes().forEach(requestAttributes::put));
            }

            String msgId = ctx.request().getHeader(StdHeaders.get().msgId());
            String groupId = ctx.request().getHeader(StdHeaders.get().groupId());

            if (null != msgId) {
                requestAttributes.put("message.id", msgId);
            }
            if (null != groupId) {
                requestAttributes.put("group.id", groupId);
            }
            return requestAttributes;
        }
    }
}
