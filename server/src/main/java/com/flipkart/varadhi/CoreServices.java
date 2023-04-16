package com.flipkart.varadhi;

import com.flipkart.varadhi.configs.ServerConfiguration;
import com.flipkart.varadhi.handlers.AuthHandlers;
import com.flipkart.varadhi.handlers.v1.HealthCheckHandler;
import com.flipkart.varadhi.handlers.v1.TopicHandlers;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Vertx;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Getter
public class CoreServices {

    @Getter(AccessLevel.PRIVATE)
    private final OpenTelemetry openTelemetry;

    private final AuthHandlers authHandlers;
    private final TopicHandlers topicHandlers;
    private final HealthCheckHandler healthCheckHandler;

    public CoreServices(OpenTelemetry openTelemetry, Vertx vertx, ServerConfiguration configuration) {
        this.openTelemetry = openTelemetry;
        this.authHandlers = new AuthHandlers(vertx, configuration);
        this.topicHandlers = new TopicHandlers();
        this.healthCheckHandler = new HealthCheckHandler();
    }

    public List<RouteDefinition> getRouteDefinitions() {
        return Stream.of(
                        topicHandlers.get(),
                        healthCheckHandler.get()
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public Tracer getTracer(String instrumentationScope, String version) {
        return getOpenTelemetry().getTracer(instrumentationScope, version);
    }
}
