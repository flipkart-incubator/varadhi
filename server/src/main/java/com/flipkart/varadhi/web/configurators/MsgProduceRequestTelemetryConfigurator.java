package com.flipkart.varadhi.web.configurators;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.config.MetricsOptions;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.metrics.ApiMetrics;
import com.flipkart.varadhi.web.routes.RouteConfigurator;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.TelemetryType;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.semconv.HttpAttributes;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import jakarta.ws.rs.BadRequestException;
import lombok.RequiredArgsConstructor;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

@Slf4j (topic = "ProduceRequestLogs")
@ExtensionMethod ({Extensions.RoutingContextExtension.class})
@RequiredArgsConstructor
public class MsgProduceRequestTelemetryConfigurator implements RouteConfigurator {

    private final SpanProvider spanProvider;
    private final MeterRegistry meterRegistry;
    private final MetricsOptions options;

    /*
        Per topic metric.
     */
    private final Map<String, ApiMetrics> metrics = new ConcurrentHashMap<>();

    @Override
    public void configure(Route route, RouteDefinition routeDef) {
        route.handler(ctx -> {
            addRequestSpanAndLog(ctx, "server.request." + routeDef.getName(), routeDef.getTelemetryType());
            ctx.next();
        });
    }

    public void addRequestSpanAndLog(RoutingContext ctx, String apiName, TelemetryType telemetryType) {
        long start = System.currentTimeMillis();
        String topic = ctx.request().getParam("topic");
        String project = ctx.request().getParam("project");

        if (Strings.isNullOrEmpty(topic) || Strings.isNullOrEmpty(project)) {
            throw new BadRequestException("Missing required parameters: topic or project");
        }

        String fqn = VaradhiTopic.fqn(project, topic);
        Span span = telemetryType.traces() ? addRequestSpan(apiName, fqn) : null;

        ctx.addEndHandler(ar -> {
            int responseCode = ctx.response().getStatusCode();
            long latencyMs = System.currentTimeMillis() - start;
            if (telemetryType.metrics()) {
                captureMetrics(apiName, fqn, latencyMs, responseCode);
            }

            if (span != null || telemetryType.logs()) {
                AttributesBuilder attributesBuilder = ctx.getRequestAttributes();
                attributesBuilder.put("user_id", ctx.getIdentityOrDefault());

                // TODO: add the error details if failed in the attributes.

                Attributes attributes = attributesBuilder.build();
                if (span != null) {
                    closeRequestSpan(span, responseCode, latencyMs, attributes);
                }
                if (telemetryType.logs()) {
                    logRequestInfo(apiName, responseCode, latencyMs, attributes);
                }
            }
        });
    }

    private Span addRequestSpan(String apiName, String topicFQN) {
        return spanProvider.newSpan(apiName).setSpanKind(SpanKind.SERVER).setAttribute("topic", topicFQN).startSpan();
    }

    private void closeRequestSpan(Span span, int responseCode, long latencyMs, Attributes requestTags) {
        span.setAttribute(HttpAttributes.HTTP_RESPONSE_STATUS_CODE, responseCode);
        span.setAttribute(AttributeKey.longKey("http.response.latency"), latencyMs);
        span.setAllAttributes(requestTags);
        span.end();
    }

    private void captureMetrics(String apiName, String topicFQN, long latencyMs, int responseCode) {
        ApiMetrics topicMetrics = metrics.get(topicFQN);
        if (topicMetrics == null) {
            topicMetrics = metrics.computeIfAbsent(
                topicFQN,
                key -> new ApiMetrics(
                    () -> Timer.builder(apiName + ".latency")
                               .tag("topic", topicFQN)
                               .publishPercentiles(options.getLatencyPercentiles())
                               .publishPercentileHistogram(false)
                               .distributionStatisticBufferLength(1024)
                               .distributionStatisticExpiry(Duration.ofMinutes(1))
                               .register(meterRegistry)
                )
            );
        }

        topicMetrics.getApiLatencyTimer().record(latencyMs, TimeUnit.MILLISECONDS);

        // TODO: careful with the -1 response code.
        topicMetrics.getResponseCounter(
            responseCode,
            (r) -> Counter.builder(apiName + ".response")
                          .tag("topic", topicFQN)
                          .tag("status_code", r / 100 + "xx")
                          .register(meterRegistry)
        ).increment();
    }

    private void logRequestInfo(String apiName, int responseCode, long latencyMs, Attributes requestTags) {
        StringBuilder sb = new StringBuilder(apiName);
        sb.append(" status_code=%d".formatted(responseCode));
        sb.append(" latency=%dms".formatted(latencyMs));
        requestTags.forEach((k, v) -> sb.append(" ").append(k.getKey()).append("=").append(v));
        log.info(sb.toString());
    }
}
