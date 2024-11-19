package com.flipkart.varadhi.web;

import com.flipkart.varadhi.exceptions.*;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import lombok.extern.slf4j.Slf4j;

import java.lang.IllegalArgumentException;

import static java.net.HttpURLConnection.*;

@Slf4j
public class FailureHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext ctx) {
        HttpServerResponse response = ctx.response();

        if (!response.ended()) {
            int statusCode =
                    overrideStatusCode(ctx.statusCode()) ? getStatusCodeFromFailure(ctx.failure()) : ctx.statusCode();
            String errorMsg =
                    overWriteErrorMsg(response) ? getErrorFromFailure(ctx.failure(), statusCode) :
                            response.getStatusMessage();
            String failureLog =
                    String.format("%s: %s: Failed. Status:%s, Error:%s", ctx.request().method(), ctx.request().path(),
                            statusCode, errorMsg
                    );
            if (statusCode == HTTP_INTERNAL_ERROR) {
                log.error(failureLog, ctx.failure());
            } else {
                log.error(failureLog);
            }
            response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            response.putHeader(HttpHeaders.CONTENT_ENCODING, "utf-8");
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
        return null == response.getStatusMessage()
                || response.getStatusMessage().isBlank()
                || response.getStatusMessage().equalsIgnoreCase(HttpResponseStatus.OK.reasonPhrase());
    }

    private String getErrorFromFailure(Throwable t, int statusCode) {
        if (t instanceof HttpException he) {
            return he.getPayload();
        } else {
            StringBuilder sb = new StringBuilder();
            if (null != t) {
                sb.append(t.getMessage());
                // include second level exception details when available and outermost exception is of not known type
                // i.e. it doesn't extend from VaradhiException.
                if (!(t instanceof VaradhiException)) {
                    if (null != t.getCause()) {
                        sb.append("Internal error : ");
                        sb.append(t.getCause().getMessage());
                    }
                }
            } else {
                sb.append(getDefaultErrorMessageFromStatusCode(statusCode));
            }
            return sb.toString();
        }
    }

    private String getDefaultErrorMessageFromStatusCode(int statusCode) {
        return switch (statusCode) {
            case HTTP_ENTITY_TOO_LARGE -> "Entity too large.";
            default -> "Internal error.";
        };
    }

    private int getStatusCodeFromFailure(Throwable t) {
        //TODO:: review produceStatus code mapping for correctness.
        Class tClazz = t.getClass();
        if (t instanceof HttpException he) {
            return he.getStatusCode();
        } else if (DuplicateResourceException.class == tClazz) {
            return HTTP_CONFLICT;
        } else if (ServerNotAvailableException.class == tClazz) {
            return HTTP_UNAVAILABLE;
        } else if (IllegalArgumentException.class == tClazz) {
            return HTTP_BAD_REQUEST;
        } else if (ResourceNotFoundException.class == tClazz) {
            return HTTP_NOT_FOUND;
        } else if (InvalidOperationForResourceException.class == tClazz) {
            return HTTP_CONFLICT;
        } else if (UnsupportedOperationException.class == tClazz) {
            return HTTP_NOT_IMPLEMENTED;
        }
        return HTTP_INTERNAL_ERROR;
    }

}
