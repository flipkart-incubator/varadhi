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

import static java.net.HttpURLConnection.*;

@Slf4j
public class FailureHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext ctx) {
        HttpServerResponse response = ctx.response();

        if (!response.ended()) {
            int statusCode = 500 == ctx.statusCode() || ctx.statusCode() < 0 ? getStatusCodeFromFailure(ctx.failure()) :
                    ctx.statusCode();
            String errorMsg =
                    overWriteErrorMsg(response) ? getErrorFromFailure(ctx.failure()) : response.getStatusMessage();

            log.error("{}: {}: Failed. Status:{}, Error:{}", ctx.request().method(), ctx.request().path(),
                    statusCode,
                    errorMsg
            );
            response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            response.putHeader(HttpHeaders.CONTENT_ENCODING, "utf-8");
            response.setStatusCode(statusCode);
            response.setStatusMessage(errorMsg);
            response.end(Json.encodeToBuffer(new ErrorResponse(errorMsg)));
        }
    }

    private boolean overWriteErrorMsg(HttpServerResponse response) {
        return null == response.getStatusMessage()
                || response.getStatusMessage().isBlank()
                || response.getStatusMessage().equalsIgnoreCase(HttpResponseStatus.OK.reasonPhrase());
    }

    private String getErrorFromFailure(Throwable t) {
        if (t instanceof HttpException he) {
            return he.getPayload();
        } else {
            StringBuilder sb = new StringBuilder();
            if (null != t) {
                sb.append(t.getMessage());
                // include second level exception, in case it is available in case exceptions have been wrapped
                // with more generic exception.
                if (null != t.getCause()) {
                    sb.append("Internal error : ");
                    sb.append(t.getCause().getMessage());
                }
            } else {
                sb.append("Internal error.");
            }
            return sb.toString();
        }
    }

    private int getStatusCodeFromFailure(Throwable t) {
        //TODO:: review status status mapping for correctness.
        Class tClazz = t.getClass();
        if (t instanceof HttpException he) {
            return he.getStatusCode();
        } else if (DuplicateResourceException.class == tClazz) {
            return HTTP_CONFLICT;
        } else if (NotImplementedException.class == tClazz) {
            return HTTP_NOT_IMPLEMENTED;
        } else if (ServerNotAvailableException.class == tClazz) {
            return HTTP_UNAVAILABLE;
        } else if (ArgumentException.class == tClazz) {
            return HTTP_BAD_REQUEST;
        } else if (ResourceNotFoundException.class == tClazz) {
            return HTTP_NOT_FOUND;
        }
        return HTTP_INTERNAL_ERROR;
    }


}
