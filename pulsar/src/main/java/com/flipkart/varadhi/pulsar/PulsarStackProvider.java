package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.services.PulsarTopicService;
import com.flipkart.varadhi.spi.services.*;
import com.flipkart.varadhi.utils.YamlLoader;


public class PulsarStackProvider implements MessagingStackProvider {
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

    public <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory() {
        if (!initialised) {
            throw new InvalidStateException("PulsarStackProvider is not yet initialised.");
        }
        return (StorageTopicFactory) this.pulsarTopicFactory;
    }

    public <T extends StorageTopic> StorageTopicService<T> getStorageTopicService() {
        if (!initialised) {
            throw new InvalidStateException("PulsarStackProvider is not yet initialised.");
        }
        return (StorageTopicService) this.pulsarTopicService;
    }

    public <T extends StorageTopic> ProducerFactory<T> getProducerFactory() {
        if (!initialised) {
            throw new InvalidStateException("PulsarStackProvider is not yet initialised.");
        }
        return (ProducerFactory) this.pulsarProducerFactory;
    }

    private void registerSubtypes(ObjectMapper mapper) {
        mapper.registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
    }

    ClientProvider getPulsarClientProvider(PulsarConfig config) {
        return new ClientProvider(config);
    }

    private PulsarConfig getPulsarConfig(String file) {
        //TODO:: Move validations to validator.
        PulsarConfig pulsarConfig = YamlLoader.loadConfig(file, PulsarConfig.class);
        if (pulsarConfig.getPulsarAdminOptions() == null) {
            throw new InvalidConfigException("Missing Pulsar Admin client configuration.");
        }
        if (pulsarConfig.getPulsarClientOptions() == null) {
            throw new InvalidConfigException("Missing Pulsar client configuration.");
        }
        return pulsarConfig;
    }
}
