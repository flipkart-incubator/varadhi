package com.flipkart.varadhi.web;

import com.flipkart.varadhi.common.exceptions.*;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.extern.slf4j.Slf4j;


import static java.net.HttpURLConnection.*;

@Slf4j
public class FailureHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext ctx) {
        HttpServerResponse response = ctx.response();

        if (!response.ended()) {
            int statusCode = overrideStatusCode(ctx.statusCode()) ?
                getStatusCodeFromFailure(ctx.failure()) :
                ctx.statusCode();
            String errorMsg = overWriteErrorMsg(response) ?
                getErrorFromFailure(ctx.failure(), statusCode) :
                response.getStatusMessage();
            String failureLog = String.format(
                "%s: %s: Failed. Status:%s, Error:%s",
                ctx.request().method(),
                ctx.request().path(),
                statusCode,
                errorMsg
            );
            if (statusCode == HTTP_INTERNAL_ERROR) {
                log.error(failureLog, ctx.failure());
            } else {
                log.error(failureLog);
            }
            response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            response.setStatusCode(statusCode);
            response.setStatusMessage(errorMsg);
            response.end(Json.encodeToBuffer(new ErrorResponse(errorMsg)));
        }
    }

    private boolean overrideStatusCode(int statusCode) {
        // override if not set or its default failure code (set by Vertx before invoking default failure handler)
        return statusCode < 0 || statusCode == HTTP_INTERNAL_ERROR;
    }

    private boolean overWriteErrorMsg(HttpServerResponse response) {
        return null == response.getStatusMessage() || response.getStatusMessage().isBlank() || response
                                                                                                       .getStatusMessage()
                                                                                                       .equalsIgnoreCase(
                                                                                                           HttpResponseStatus.OK.reasonPhrase()
                                                                                                       );
    }

    private String getErrorFromFailure(Throwable t, int statusCode) {
        HttpException httpException = findCause(t, HttpException.class);
        if (httpException != null) {
            return httpException.getPayload();
        }
        VaradhiException varadhiException = findCause(t, VaradhiException.class);
        if (varadhiException != null) {
            return varadhiException.getMessage();
        }
        if (null != t) {
            return t.getMessage() != null ? t.getMessage() : getDefaultErrorMessageFromStatusCode(statusCode);
        }
        return getDefaultErrorMessageFromStatusCode(statusCode);
    }

    private String getDefaultErrorMessageFromStatusCode(int statusCode) {
        return switch (statusCode) {
            case HTTP_ENTITY_TOO_LARGE -> "Entity too large.";
            default -> "Internal error.";
        };
    }

    private int getStatusCodeFromFailure(Throwable t) {
        HttpException httpException = findCause(t, HttpException.class);
        if (httpException != null) {
            return httpException.getStatusCode();
        }
        if (findCause(t, DuplicateResourceException.class) != null) {
            return HTTP_CONFLICT;
        }
        if (findCause(t, ServerNotAvailableException.class) != null) {
            return HTTP_UNAVAILABLE;
        }
        if (findCause(t, IllegalArgumentException.class) != null) {
            return HTTP_BAD_REQUEST;
        }
        if (findCause(t, ResourceNotFoundException.class) != null) {
            return HTTP_NOT_FOUND;
        }
        if (findCause(t, InvalidOperationForResourceException.class) != null) {
            return HTTP_CONFLICT;
        }
        if (findCause(t, UnsupportedOperationException.class) != null) {
            return HTTP_NOT_IMPLEMENTED;
        }
        return HTTP_INTERNAL_ERROR;
    }

    private static <T extends Throwable> T findCause(Throwable t, Class<T> type) {
        while (t != null) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
            t = t.getCause();
        }
        return null;
    }

}
