package com.flipkart.varadhi.web;

import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.TelemetryType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.semconv.SemanticAttributes;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_IDENTITY;
import static com.flipkart.varadhi.entities.StandardHeaders.GROUP_ID;
import static com.flipkart.varadhi.entities.StandardHeaders.MESSAGE_ID;

@Slf4j(topic = "RequestLogs")
public class RequestTelemetryConfigurator implements RouteConfigurator {
    public static final String REQUEST_SPAN_NAME = "server.request";
    private final SpanProvider spanProvider;

    // TODO: remove explicit dependency on meterRegistry. create a separate class for metrics
    private final MeterRegistry meterRegistry;

    public RequestTelemetryConfigurator(SpanProvider spanProvider, MeterRegistry meterRegistry) {
        this.spanProvider = spanProvider;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(ctx -> {
            addRequestSpanAndLog(ctx, routeDef.getName(), routeDef.getTelemetryType());
            ctx.next();
        });
    }

    public void addRequestSpanAndLog(RoutingContext ctx, String apiName, TelemetryType telemetryType) {
        long start = System.currentTimeMillis();
        Span span = telemetryType.traces() ? addRequestSpan(apiName) : null;
        ctx.response().endHandler(r -> {
            String identity = getIdentity(ctx);
            String resource = getResourceHierarchy(ctx);
            int responseCode = ctx.response().getStatusCode();
            long latencyMs = System.currentTimeMillis() - start;

            if(telemetryType.metrics()) {
                timeRequest(apiName, resource, responseCode, latencyMs);
            }

            MessageInfo messageInfo = null;
            if(null != span || telemetryType.logs()) {
                messageInfo = getMessageInfo(ctx);
            }

            if (null != span) {
                closeRequestSpan(span, identity, resource, messageInfo, responseCode, latencyMs);
            }
            if(telemetryType.logs()) {
                logRequestInfo(apiName, identity, resource, messageInfo, responseCode, latencyMs);
            }
        });
    }

    private void timeRequest(String apiName, String resource, int responseCode, long latencyMs) {
        Timer.builder(apiName + ".latency")
                .tag("resource", resource)
                .register(meterRegistry)
                .record(latencyMs, TimeUnit.MILLISECONDS);

        Counter.builder(apiName + ".status_code")
                .tag("category", responseCode / 100 + "xx")
                .register(meterRegistry)
                .increment();
    }

    private Span addRequestSpan(String apiName) {
        return spanProvider.addSpan(REQUEST_SPAN_NAME).setAttribute("api", apiName);
    }

    private void closeRequestSpan(Span span, String identity, String resource, MessageInfo messageInfo, int responseCode, long latencyMs) {
        // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/general/attributes.md#general-identity-attributes
        span.setAttribute(SemanticAttributes.ENDUSER_ID, identity);
        span.setAttribute(SemanticAttributes.HTTP_REQUEST_BODY_SIZE, messageInfo.payloadSize);
        span.setAttribute(SemanticAttributes.HTTP_STATUS_CODE, responseCode);
        span.setAttribute(AttributeKey.longKey("http.request.latency"), latencyMs);
        span.setAttribute("resource", resource);

        if(messageInfo.messageId != null) {
            span.setAttribute("message.id", messageInfo.messageId);
        }
        if (messageInfo.groupId != null) {
            span.setAttribute("group.id", messageInfo.groupId);
        }

        span.end();
    }

    private void logRequestInfo(
            String apiName, String identity, String resource, MessageInfo messageInfo, int responseCode, long latencyMs
    ) {
        if (null == messageInfo) {
            log.info("server.request api={} identity={} resource={} status={} latency={}ms", apiName, identity,
                    resource, responseCode, latencyMs
            );
        } else {
            log.info("server.request api={} identity={} resource={} messageInfo={} status={} latency={}ms", apiName,
                    identity, resource, messageInfo, responseCode, latencyMs
            );
        }
    }

    private String getIdentity(RoutingContext ctx) {
        User user = ctx.user();
        return null != user ? user.subject() : ANONYMOUS_IDENTITY;
    }

    private String getResourceHierarchy(RoutingContext ctx) {
        ResourceHierarchy resourceHierarchy = ctx.get(CONTEXT_KEY_RESOURCE_HIERARCHY);
        return null != resourceHierarchy ? resourceHierarchy.getResourcePath() : "Unknown_Resource_Spec";
    }

    private MessageInfo getMessageInfo(RoutingContext ctx) {
        String msgId = ctx.request().getHeader(MESSAGE_ID);
        String groupId = ctx.request().getHeader(GROUP_ID);
        int payloadSize = ctx.body() != null ? ctx.body().length() : 0;
        return new MessageInfo(groupId, msgId, payloadSize);
    }

    private record MessageInfo (String groupId, String messageId, int payloadSize) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (groupId != null) {
                sb.append("group.id='").append(groupId).append('\'');
            }
            if (messageId != null) {
                if (!sb.isEmpty()) {
                    sb.append(",");
                }
                sb.append("message.id='").append(messageId).append('\'');
            }
            if (!sb.isEmpty()) {
                sb.append(",");
            }
            sb.append("payload.size=").append(payloadSize);
            return sb.toString();
        }
    }
}
