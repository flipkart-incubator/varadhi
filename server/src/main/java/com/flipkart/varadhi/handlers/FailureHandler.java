package com.flipkart.varadhi.handlers;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

@Slf4j
public class FailureHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext event) {
        HttpServerResponse response = event.response();

        int statusCode = -1 == event.statusCode() ? HTTP_INTERNAL_ERROR : event.statusCode();
        String errorMsg =
                overWriteErrorMsg(response) ? getErrorFromFailure(event.failure()) : response.getStatusMessage();

        response.setStatusCode(statusCode);
        response.setStatusMessage(errorMsg);

        log.error(
                "{}: {}: Failed. Status:{}, Error:{}", event.request().method(), event.request().path(), statusCode,
                errorMsg
        );
        if (!response.ended()) {
            response.end();
        }
    }

    private boolean overWriteErrorMsg(HttpServerResponse response) {
        return null == response.getStatusMessage() ||
                response.getStatusMessage().isBlank() ||
                response.getStatusMessage().equalsIgnoreCase(HttpResponseStatus.OK.reasonPhrase());
    }

    private String getErrorFromFailure(Throwable t) {
        StringBuilder sb = new StringBuilder();
        if (null != t) {
            sb.append(t.getMessage());
            // include second level exception, in case it is available in case exceptions have been wrapped
            // with more generic exception.
            if (null != t.getCause()) {
                sb.append(" ( Internal error : ");
                sb.append(t.getCause().getMessage());
                sb.append(" )");
            }
        } else {
            sb.append("Unknown Error");
        }
        return sb.toString();
    }
}
