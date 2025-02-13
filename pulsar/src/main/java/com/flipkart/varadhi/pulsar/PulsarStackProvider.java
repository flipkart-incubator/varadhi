package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.consumer.PulsarConsumerFactory;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.entities.PulsarSubscription;
import com.flipkart.varadhi.pulsar.producer.PulsarProducerFactory;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.*;
import com.flipkart.varadhi.utils.HostUtils;
import com.flipkart.varadhi.utils.YamlLoader;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class PulsarStackProvider implements
    MessagingStackProvider<PulsarStorageTopic, PulsarOffset, PulsarSubscription> {
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
        producerFactory = new PulsarProducerFactory(
            clientProvider.getPulsarClient(),
            pulsarConfig.getProducerOptions(),
            hostName
        );
        consumerFactory = new PulsarConsumerFactory(clientProvider.getPulsarClient(), new HashMap<>());
        subscriptionFactory = new PulsarSubscriptionFactory();
        subscriptionService = new PulsarSubscriptionService(clientProvider);
        registerSubtypes(mapper);
        initialised = true;
    }

    public StorageTopicFactory<PulsarStorageTopic> getStorageTopicFactory() {
        ensureInitialized();
        return topicFactory;
    }

    public StorageSubscriptionFactory<PulsarSubscription, PulsarStorageTopic> getSubscriptionFactory() {
        ensureInitialized();
        return subscriptionFactory;
    }

    public StorageTopicService<PulsarStorageTopic> getStorageTopicService() {
        ensureInitialized();
        return topicService;
    }

    public StorageSubscriptionService<PulsarSubscription> getStorageSubscriptionService() {
        ensureInitialized();
        return subscriptionService;
    }

    public ProducerFactory<PulsarStorageTopic> getProducerFactory() {
        ensureInitialized();
        return producerFactory;
    }

    @Override
    public ConsumerFactory<PulsarStorageTopic, PulsarOffset> getConsumerFactory() {
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
