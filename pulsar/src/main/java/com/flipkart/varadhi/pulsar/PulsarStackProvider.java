package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.producer.PulsarProducerFactory;
import com.flipkart.varadhi.spi.services.*;
import com.flipkart.varadhi.utils.YamlLoader;


public class PulsarStackProvider implements MessagingStackProvider<PulsarStorageTopic, PulsarOffset> {
    private PulsarTopicService pulsarTopicService;
    private PulsarTopicFactory pulsarTopicFactory;
    private PulsarProducerFactory pulsarProducerFactory;
    private volatile boolean initialised = false;

    @Override
    public String getName() {
        return "pulsar";
    }

    public synchronized void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper) {
        if (initialised) {
            return;
        }

        PulsarConfig pulsarConfig = getPulsarConfig(messagingStackOptions.getConfigFile());
        pulsarTopicFactory = new PulsarTopicFactory();
        ClientProvider clientProvider = new ClientProvider(pulsarConfig);
        pulsarTopicService = new PulsarTopicService(clientProvider);
        //TODO:: Fix hostname. Get it using hostutils.
        String hostName = "Undefined.TobeFixed";
        pulsarProducerFactory =
                new PulsarProducerFactory(
                        clientProvider.getPulsarClient(), pulsarConfig.getProducerOptions(), hostName);
        registerSubtypes(mapper);
        initialised = true;
    }

    public StorageTopicFactory<PulsarStorageTopic> getStorageTopicFactory() {
        ensureInitialized();
        return this.pulsarTopicFactory;
    }

    public StorageTopicService<PulsarStorageTopic> getStorageTopicService() {
        ensureInitialized();
        return this.pulsarTopicService;
    }

    public ProducerFactory<PulsarStorageTopic> getProducerFactory() {
        ensureInitialized();
        return this.pulsarProducerFactory;
    }

    @Override
    public ConsumerFactory<PulsarStorageTopic, PulsarOffset> getConsumerFactory() {
        return null;
    }

    private void registerSubtypes(ObjectMapper mapper) {
        mapper.registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
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
