package com.flipkart.varadhi.web.core.configurators;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.flipkart.varadhi.produce.config.MetricsOptions;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.metrics.ApiMetrics;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.TelemetryType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.semconv.HttpAttributes;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import lombok.RequiredArgsConstructor;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

@Slf4j (topic = "RequestLogs")
@ExtensionMethod ({Extensions.RoutingContextExtension.class})
@RequiredArgsConstructor
public class RequestTelemetryConfigurator implements RouteConfigurator {

    private final SpanProvider spanProvider;
    private final MeterRegistry meterRegistry;
    private final MetricsOptions options;

    public static RequestTelemetryConfigurator getDefault(SpanProvider spanProvider) {
        return new RequestTelemetryConfigurator(spanProvider, new SimpleMeterRegistry(), MetricsOptions.getDefault());
    }

    /*
        Per API metric.
     */
    private final Map<String, ApiMetrics> metrics = new ConcurrentHashMap<>();

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
            int responseCode = ctx.response().getStatusCode();
            long latencyMs = System.currentTimeMillis() - start;

            if (telemetryType.metrics()) {
                captureMetrics(apiName, latencyMs, responseCode);
            }

            if (span != null || telemetryType.logs()) {
                AttributesBuilder attributesBuilder = ctx.getRequestAttributes();
                attributesBuilder.put("user_id", ctx.getIdentityOrDefault());

                // TODO: add the error details if failed in the attributes.

                Attributes attributes = attributesBuilder.build();
                if (span != null) {
                    closeRequestSpan(span, ctx, responseCode, latencyMs, attributes);
                }
                if (telemetryType.logs()) {
                    logRequestInfo(apiName, responseCode, latencyMs, attributes);
                }
            }
        });
    }

    private Span addRequestSpan(String apiName) {
        return spanProvider.newSpan("server.request")
                           .setSpanKind(SpanKind.SERVER)
                           .setAttribute("api", apiName)
                           .startSpan();
    }

    private void closeRequestSpan(
        Span span,
        RoutingContext ctx,
        int responseCode,
        long latencyMs,
        Attributes requestTags
    ) {
        span.setAttribute(HttpAttributes.HTTP_REQUEST_METHOD, ctx.request().method().name());
        span.setAttribute(AttributeKey.stringKey("url.path"), ctx.request().path());
        if (ctx.request().query() != null) {
            span.setAttribute(AttributeKey.stringKey("url.query"), ctx.request().query());
        }
        span.setAttribute(HttpAttributes.HTTP_RESPONSE_STATUS_CODE, responseCode);
        span.setAttribute(AttributeKey.longKey("http.response.latency"), latencyMs);
        span.setAllAttributes(requestTags);
        span.end();
    }

    private void captureMetrics(String apiName, long latencyMs, int responseCode) {
        ApiMetrics apiMetrics = metrics.get(apiName);
        if (apiMetrics == null) {
            apiMetrics = metrics.computeIfAbsent(
                apiName,
                key -> new ApiMetrics(
                    () -> Timer.builder("server.request.latency")
                               .tag("api", apiName)
                               .publishPercentiles(options.getLatencyPercentiles())
                               .publishPercentileHistogram(false)
                               .distributionStatisticExpiry(Duration.ofMinutes(1))
                               .register(meterRegistry)
                )
            );
        }

        apiMetrics.getApiLatencyTimer().record(latencyMs, TimeUnit.MILLISECONDS);

        // TODO: careful with the -1 response code.
        apiMetrics.getResponseCounter(
            responseCode,
            (r) -> Counter.builder("server.request.response")
                          .tag("api", apiName)
                          .tag("status_code", r / 100 + "xx")
                          .register(meterRegistry)
        ).increment();
    }

    private void logRequestInfo(String apiName, int responseCode, long latencyMs, Attributes requestTags) {
        StringBuilder sb = new StringBuilder("server.request");
        sb.append(" api=%s".formatted(apiName));
        sb.append(" status_code=%d".formatted(responseCode));
        sb.append(" latency=%dms".formatted(latencyMs));
        requestTags.forEach((k, v) -> sb.append(" ").append(k.getKey()).append("=").append(v));
        log.info(sb.toString());
    }
}
