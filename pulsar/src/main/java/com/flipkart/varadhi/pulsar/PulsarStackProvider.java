package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.services.PulsarTopicService;
import com.flipkart.varadhi.spi.services.*;
import com.flipkart.varadhi.utils.YamlLoader;


public class PulsarStackProvider implements MessagingStackProvider<PulsarStorageTopic> {
    private PulsarTopicService pulsarTopicService;
    private PulsarTopicFactory pulsarTopicFactory;
    private PulsarProducerFactory pulsarProducerFactory;
    private volatile boolean initialised = false;


    public void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper) {
        if (!initialised) {
            synchronized (this) {
                if (!initialised) {
                    PulsarConfig pulsarConfig = getPulsarConfig(messagingStackOptions.getConfigFile());
                    pulsarTopicFactory = new PulsarTopicFactory();
                    ClientProvider clientProvider = getPulsarClientProvider(pulsarConfig);
                    pulsarTopicService = new PulsarTopicService(clientProvider);
                    //TODO:: Fix hostname. Get it using hostutils.
                    String hostName = "Undefined.TobeFixed";
                    pulsarProducerFactory =
                            new PulsarProducerFactory(clientProvider, pulsarConfig.getProducerOptions(), hostName);
                    registerSubtypes(mapper);
                    initialised = true;
                }
            }
        }
    }

    public StorageTopicFactory<PulsarStorageTopic> getStorageTopicFactory() {
        if (!initialised) {
            throw new IllegalStateException("PulsarStackProvider is not yet initialised.");
        }
        return this.pulsarTopicFactory;
    }

    public StorageTopicService<PulsarStorageTopic> getStorageTopicService() {
        if (!initialised) {
            throw new IllegalStateException("PulsarStackProvider is not yet initialised.");
        }
        return this.pulsarTopicService;
    }

    public ProducerFactory<PulsarStorageTopic> getProducerFactory() {
        if (!initialised) {
            throw new IllegalStateException("PulsarStackProvider is not yet initialised.");
        }
        return this.pulsarProducerFactory;
    }

    private void registerSubtypes(ObjectMapper mapper) {
        mapper.registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
    }

    ClientProvider getPulsarClientProvider(PulsarConfig config) {
        return new ClientProvider(config);
    }

    private PulsarConfig getPulsarConfig(String file) {
        return YamlLoader.loadConfig(file, PulsarConfig.class);
    }
}
