package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import io.opentelemetry.api.trace.Span;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_IDENTITY;
import static com.flipkart.varadhi.entities.StandardHeaders.GROUP_ID;
import static com.flipkart.varadhi.entities.StandardHeaders.MESSAGE_ID;

@Slf4j
public class RequestTraceAndLogHandler implements RouteConfigurator {
    public static final String REQUEST_SPAN_NAME = "varadhi.Request";

    private final SpanProvider spanProvider;
    private final boolean traceRequestEnabled;

    public RequestTraceAndLogHandler(boolean traceRequestEnabled, SpanProvider spanProvider) {
        this.traceRequestEnabled = traceRequestEnabled;
        this.spanProvider = spanProvider;
    }

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(ctx -> addRequestSpanAndLog(ctx, routeDef.getName()));
    }

    public void addRequestSpanAndLog(RoutingContext ctx, String apiName) {
        long start = System.currentTimeMillis();
        Span span = addRequestSpan(apiName);
        ctx.response().endHandler(r -> {
            String identity = getIdentity(ctx);
            String resource = getResourceHierarchy(ctx);
            String messageInfo = getMessageInfo(ctx);
            int responseCode = ctx.response().getStatusCode();
            long latency = System.currentTimeMillis() - start;
            closeRequestSpan(span, identity, resource, messageInfo, responseCode);
            logRequestInfo(apiName, identity, resource, messageInfo, responseCode, latency);
        });
        ctx.next();
    }

    private Span addRequestSpan(String apiName) {
        if (traceRequestEnabled) {
            return spanProvider.addSpan(REQUEST_SPAN_NAME).setAttribute("api", apiName);
        }
        return null;
    }

    private void closeRequestSpan(Span span, String identity, String resource, String messageInfo, int responseCode) {
        if (traceRequestEnabled) {
            if (null != span) {
                span.setAttribute("identity", identity);
                span.setAttribute("resource", resource);
                span.setAttribute("messageInfo", messageInfo); // attributes with null value is ignored.
                span.setAttribute("status", responseCode);
                span.end();
            }
        }
    }

    private void logRequestInfo(
            String apiName, String identity, String resource, String messageInfo, int responseCode, long latency
    ) {
        if (null == messageInfo) {
            log.info("varadhi.Request api={} identity={} resource={} status={} latency={}ms", apiName, identity,
                    resource, responseCode, latency
            );
        } else {
            log.info("varadhi.Request api={} identity={} resource={} messageInfo={} status={} latency={}ms", apiName,
                    identity, resource, messageInfo, responseCode, latency
            );
        }
    }

    private String getIdentity(RoutingContext ctx) {
        User user = ctx.user();
        return null != user ? user.subject() : ANONYMOUS_IDENTITY;
    }

    private String getResourceHierarchy(RoutingContext ctx) {
        ResourceHierarchy resourceHierarchy = ctx.get(CONTEXT_KEY_RESOURCE_HIERARCHY);
        return null != resourceHierarchy ? resourceHierarchy.getResourcePath() : String.format("(%s) - unresolved path.", ctx.request().path());
    }

    private String getMessageInfo(RoutingContext ctx) {
        if (null != ctx.request().getHeader(MESSAGE_ID)) {
            StringBuilder sb = new StringBuilder();
            sb.append(ctx.request().getHeader(MESSAGE_ID));
            if (null != ctx.request().getHeader(GROUP_ID)) {
                sb.append(",").append(ctx.request().getHeader(GROUP_ID));
            }
            sb.append(",").append(ctx.body().length());
            return sb.toString();
        }
        return null;
    }
}
