package com.flipkart.varadhi;


import com.flipkart.varadhi.db.MetaStoreOptions;
import com.flipkart.varadhi.db.MetaStoreProvider;
import com.flipkart.varadhi.entities.VaradhiTopicFactory;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.services.MessagingStackProvider;
import com.flipkart.varadhi.services.MessagingStackOptions;
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
        MessagingStackProvider messagingStackProvider = getMessagingStackProvider(configuration.getMessagingStackOptions());
        MetaStoreProvider metaStoreProvider = getMetaStoreProvider(configuration.getMetaStoreOptions());
        VaradhiTopicFactory topicFactory = new VaradhiTopicFactory(messagingStackProvider.getStorageTopicFactory());
        VaradhiTopicService topicService = new VaradhiTopicService(
                messagingStackProvider.getStorageTopicService(),
                metaStoreProvider.getMetaStore()
        );
        this.topicHandlers = new TopicHandlers(topicFactory, topicService, metaStoreProvider.getMetaStore());
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

    private MetaStoreProvider getMetaStoreProvider(MetaStoreOptions metaStoreOptions) {
        MetaStoreProvider provider = loadClass(metaStoreOptions.getProviderClassName());
        provider.init(metaStoreOptions);
        return provider;
    }


    private MessagingStackProvider getMessagingStackProvider(MessagingStackOptions messagingStackOptions) {
        MessagingStackProvider provider = loadClass(messagingStackOptions.getProviderClassName());
        provider.init(messagingStackOptions);
        return provider;
    }

    private <T> T loadClass(String className) {
        try{
            if (null != className && !className.isBlank()) {
                Class<T> pluginClass = (Class<T>) Class.forName(className);
                return pluginClass.getDeclaredConstructor().newInstance();
            }
            throw new InvalidConfigException("No class provided.");
        }catch(Exception e) {
            String errorMsg = String.format("Fail to load class %s.", className);
            log.error(errorMsg, e);
            throw new InvalidConfigException(e);
        }
    }





    @Getter
    @AllArgsConstructor
    public static class ObservabilityStack {
        private final OpenTelemetry openTelemetry;
        private final MeterRegistry meterRegistry;
    }
}
