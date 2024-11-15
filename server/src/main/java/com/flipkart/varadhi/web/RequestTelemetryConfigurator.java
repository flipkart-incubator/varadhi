package com.flipkart.varadhi.web;


import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.TelemetryType;
import io.micrometer.core.instrument.*;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.flipkart.varadhi.entities.StandardHeaders.GROUP_ID;
import static com.flipkart.varadhi.entities.StandardHeaders.MESSAGE_ID;

@Slf4j(topic = "RequestLogs")
@ExtensionMethod({Extensions.RoutingContextExtension.class})
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
            String apiPath = ctx.request().path();
            int responseCode = ctx.response().getStatusCode();
            long latencyMs = System.currentTimeMillis() - start;
            Map<String, String> requestAttributes = ctx.getRequestAttributes();
            requestAttributes.put("user_id", ctx.getIdentityOrDefault());
            List<Tag> requestTags =
                    requestAttributes.entrySet().stream().map(es -> Tag.of(es.getKey(), es.getValue()))
                            .collect(Collectors.toList());

            if (telemetryType.metrics()) {
                timeRequest(apiName, requestTags, latencyMs);
                countRequestStatusCode(apiName, requestTags, responseCode);
            }
            requestTags.addAll(getMessageTags(ctx));
            if (null != span) {
                closeRequestSpan(span, apiPath, responseCode, latencyMs, requestTags);
            }
            if (telemetryType.logs()) {
                logRequestInfo(apiPath, responseCode, latencyMs, apiName, requestTags);
            }
        });
    }

    private void timeRequest(String apiName, List<Tag> tags, long latencyMs) {
        Timer.builder(apiName + ".latency")
                .tags(tags)
                .register(meterRegistry)
                .record(latencyMs, TimeUnit.MILLISECONDS);
    }

    private void countRequestStatusCode(String apiName, List<Tag> tags, int responseCode) {
        Counter.builder(apiName + ".status_code")
                .tag("category", responseCode / 100 + "xx")
                .tags(tags)
                .register(meterRegistry)
                .increment();
    }

    private Span addRequestSpan(String apiName) {
        return spanProvider.addSpan(REQUEST_SPAN_NAME).setAttribute("api", apiName);
    }

    private void closeRequestSpan(
            Span span, String apiPath, int responseCode, long latencyMs, List<Tag> requestTags
    ) {
        // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/general/attributes.md#general-identity-attributes
        span.setAttribute(AttributeKey.stringKey("http.path"), apiPath);
        span.setAttribute(AttributeKey.longKey("http.status_code"), responseCode);
        span.setAttribute(AttributeKey.stringKey("http.status_code.category"), responseCode / 100 + "xx");
        span.setAttribute(AttributeKey.longKey("http.request.latency"), latencyMs);
        requestTags.forEach(t -> span.setAttribute(t.getKey(), t.getValue()));
        span.end();
    }

    private void logRequestInfo(
            String apiPath, int responseCode, long latencyMs, String apiName, List<Tag> requestTags
    ) {
        StringBuilder sb = new StringBuilder("server.request");
        sb.append(" apiPath=%s".formatted(apiPath));
        sb.append(" status=%d".formatted(responseCode));
        sb.append(" latency=%dms".formatted(latencyMs));
        sb.append(" apiName=%s".formatted(apiName));
        requestTags.forEach(t -> sb.append(" ").append(t.getKey()).append("=").append(t.getValue()));
        log.info(sb.toString());
    }

    private List<Tag> getMessageTags(RoutingContext ctx) {
        List<Tag> tags = new ArrayList<>();
        if (null != ctx.request().getHeader(MESSAGE_ID)) {
            tags.add(Tag.of("message.id", ctx.request().getHeader(MESSAGE_ID)));
        }
        if (null != ctx.request().getHeader(GROUP_ID)) {
            tags.add(Tag.of("group.id", ctx.request().getHeader(GROUP_ID)));
        }
        tags.add(Tag.of("payload.size", String.valueOf(ctx.body() != null ? ctx.body().length() : 0)));
        return tags;
    }
}
