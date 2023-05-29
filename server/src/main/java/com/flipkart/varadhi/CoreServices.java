package com.flipkart.varadhi;

import com.flipkart.varadhi.db.*;
import com.flipkart.varadhi.entities.VaradhiTopicFactory;
import com.flipkart.varadhi.pulsar.PulsarProvider;
import com.flipkart.varadhi.services.PlatformOptions;
import com.flipkart.varadhi.services.PlatformProvider;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.web.AuthHandlers;
import com.flipkart.varadhi.web.RouteDefinition;
import com.flipkart.varadhi.web.v1.HealthCheckHandler;
import com.flipkart.varadhi.web.v1.TopicHandlers;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
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
    private final ObservabilityStack observabilityStack;
    private final AuthHandlers authHandlers;
    private final TopicHandlers topicHandlers;
    private final HealthCheckHandler healthCheckHandler;

    private final BodyHandler bodyHandler;

    public CoreServices(ObservabilityStack observabilityStack, Vertx vertx, ServerConfiguration configuration) {
        this.observabilityStack = observabilityStack;
        this.authHandlers = new AuthHandlers(vertx, configuration);
        PlatformProvider platformProvider = getPlatformProvider(configuration.getPlatformOptions());
        PersistenceProvider persistenceProvider = getPersistenceProvider(configuration.getDbOptions());
        VaradhiTopicFactory topicFactory = new VaradhiTopicFactory(platformProvider.getStorageTopicFactory());
        VaradhiTopicService topicService = new VaradhiTopicService(platformProvider.getStorageTopicServiceFactory(),
                persistenceProvider.getPersistence());
        this.topicHandlers = new TopicHandlers(topicFactory, topicService, persistenceProvider.getPersistence());
        this.healthCheckHandler = new HealthCheckHandler();
        this.bodyHandler = BodyHandler.create(false);
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
        return getObservabilityStack().getOpenTelemetry().getTracer(instrumentationScope, version);
    }

    public MeterRegistry getMetricsRegistry() {
        return getObservabilityStack().getMeterRegistry();
    }



    /*
      TODO::Provider needs to be fixed.
       - Should be Strongly typed instead of Raw.
       - Also it should be injected dynamically.
     */

    private PersistenceProvider getPersistenceProvider(DBOptions DBOptions) {
        PersistenceProvider provider = new ZookeeperProvider();
        provider.init(DBOptions);
        return provider;
    }


    private PlatformProvider getPlatformProvider(PlatformOptions platformOptions) {
        PulsarProvider pulsarProvider = new PulsarProvider();
        pulsarProvider.init(platformOptions);
        return pulsarProvider;
    }



    @Getter
    @AllArgsConstructor
    public static class ObservabilityStack {
        private final OpenTelemetry openTelemetry;
        private final MeterRegistry meterRegistry;
    }
}
