package com.flipkart.varadhi;

import com.flipkart.varadhi.configs.ServerConfiguration;
import com.flipkart.varadhi.handlers.AuthHandlers;
import com.flipkart.varadhi.handlers.v1.HealthCheckHandler;
import com.flipkart.varadhi.handlers.v1.TopicHandlers;
import io.vertx.core.Vertx;
import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class CoreServices {

    private final AuthHandlers authHandlers;
    private final TopicHandlers topicHandlers;
    private final HealthCheckHandler healthCheckHandler;

    public CoreServices(Vertx vertx, ServerConfiguration configuration) {
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
}
