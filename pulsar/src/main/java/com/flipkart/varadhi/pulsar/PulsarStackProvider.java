package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.consumer.PulsarConsumerFactory;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.entities.PulsarSubscription;
import com.flipkart.varadhi.pulsar.producer.PulsarProducerFactory;
import com.flipkart.varadhi.pulsar.util.PulsarTelemetryOptions;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.*;
import com.flipkart.varadhi.common.utils.HostUtils;
import com.flipkart.varadhi.common.utils.YamlLoader;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PulsarStackProvider implements MessagingStackProvider {
    private PulsarTopicService topicService;
    private PulsarTopicFactory topicFactory;
    private PulsarProducerFactory producerFactory;
    private PulsarSubscriptionFactory subscriptionFactory;
    private PulsarSubscriptionService subscriptionService;
    private PulsarConsumerFactory consumerFactory;
    private volatile boolean initialised = false;

    @Override
    public String getName() {
        return "pulsar";
    }

    public synchronized void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper) {
        if (initialised) {
            return;
        }
        String hostName = HostUtils.getHostName();
        PulsarConfig pulsarConfig = getPulsarConfig(messagingStackOptions.getConfigFile());
        TopicPlanner planner = new TopicPlanner(pulsarConfig);
        topicFactory = new PulsarTopicFactory(planner);
        ClientProvider clientProvider = new ClientProvider(pulsarConfig);
        topicService = new PulsarTopicService(clientProvider, planner);
        PulsarTelemetryOptions pulsarTelemetryOptions = null;
        if (pulsarConfig.isEnableTelemetry()) {
            pulsarTelemetryOptions = new PulsarTelemetryOptions();
        }
        producerFactory = new PulsarProducerFactory(
            clientProvider.getPulsarClient(),
            pulsarConfig.getProducerOptions(),
            hostName,
            pulsarTelemetryOptions
        );
        consumerFactory = new PulsarConsumerFactory(
            clientProvider.getPulsarClient(),
            pulsarConfig.getConsumerOptions(),
            pulsarTelemetryOptions
        );
        subscriptionFactory = new PulsarSubscriptionFactory();
        subscriptionService = new PulsarSubscriptionService(clientProvider);
        registerSubtypes(mapper);
        initialised = true;
    }

    @Override
    public StorageTopicFactory<PulsarStorageTopic> getStorageTopicFactory() {
        ensureInitialized();
        return topicFactory;
    }

    @Override
    public PulsarSubscriptionFactory getSubscriptionFactory() {
        ensureInitialized();
        return subscriptionFactory;
    }

    @Override
    public StorageTopicService getStorageTopicService() {
        ensureInitialized();
        return topicService;
    }

    @Override
    public StorageSubscriptionService getStorageSubscriptionService() {
        ensureInitialized();
        return subscriptionService;
    }

    @Override
    public ProducerFactory getProducerFactory() {
        ensureInitialized();
        return producerFactory;
    }

    @Override
    public ConsumerFactory getConsumerFactory() {
        ensureInitialized();
        return consumerFactory;
    }

    private void registerSubtypes(ObjectMapper mapper) {
        mapper.registerSubtypes(new NamedType(PulsarStorageTopic.class, "PulsarTopic"));
        mapper.registerSubtypes(new NamedType(PulsarSubscription.class, "PulsarSubscription"));
        mapper.registerSubtypes(new NamedType(PulsarOffset.class, "PulsarOffset"));
    }

    private PulsarConfig getPulsarConfig(String file) {
        return YamlLoader.loadConfig(file, PulsarConfig.class);
    }

    private void ensureInitialized() {
        if (!initialised) {
            throw new IllegalStateException("PulsarStackProvider is not yet initialised.");
        }
    }
}
