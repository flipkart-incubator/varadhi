package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.entities.ProducerFactory;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.StorageTopicFactory;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.pulsar.config.PulsarClientOptions;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.services.PulsarTopicService;
import com.flipkart.varadhi.services.MessagingStackOptions;
import com.flipkart.varadhi.services.MessagingStackProvider;
import com.flipkart.varadhi.services.StorageTopicService;
import com.flipkart.varadhi.utils.YamlLoader;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;


public class PulsarStackProvider implements MessagingStackProvider {
    private PulsarTopicService pulsarTopicService;
    private PulsarTopicFactory pulsarTopicFactory;
    private PulsarProducerFactory pulsarProducerFactory;
    private volatile boolean initialised = false;
    private static final TimeUnit TimeUnitMS = TimeUnit.MILLISECONDS;

    public void init(MessagingStackOptions messagingStackOptions, ObjectMapper mapper) {
        if (!this.initialised) {
            synchronized (this) {
                if (!this.initialised) {
                    PulsarConfig pulsarConfig =
                            YamlLoader.loadConfig(messagingStackOptions.getConfigFile(), PulsarConfig.class);
                    this.pulsarTopicFactory = new PulsarTopicFactory();
                    PulsarAdmin pulsarAdmin = getPulsarAdminClient(pulsarConfig.getPulsarClientOptions());
                    this.pulsarTopicService = new PulsarTopicService(pulsarAdmin);
                    this.pulsarProducerFactory = new PulsarProducerFactory();
                    registerSubtypes(mapper);
                    this.initialised = true;
                }
            }
        }
    }

    public <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory() {
        if (!this.initialised) {
            throw new InvalidStateException("PulsarStackProvider is not yet initialised.");
        }
        return (StorageTopicFactory) this.pulsarTopicFactory;
    }

    public <T extends StorageTopic> StorageTopicService<T> getStorageTopicService() {
        if (!this.initialised) {
            throw new InvalidStateException("PulsarStackProvider is not yet initialised.");
        }
        return (StorageTopicService) this.pulsarTopicService;
    }

    public <T extends StorageTopic> ProducerFactory<T> getProducerFactory() {
        if (!this.initialised) {
            throw new InvalidStateException("PulsarStackProvider is not yet initialised.");
        }
        return (ProducerFactory) this.pulsarProducerFactory;
    }

    private void registerSubtypes(ObjectMapper mapper) {
        mapper.registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
    }

    PulsarAdmin getPulsarAdminClient(PulsarClientOptions pulsarClientOptions) {
        try {
            //TODO::Add authentication to the pulsar clients. It should be optional however.
            return PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarClientOptions.getPulsarUrl())
                    .connectionTimeout(pulsarClientOptions.getConnectTimeout(), TimeUnitMS)
                    .requestTimeout(pulsarClientOptions.getRequestTimeout(), TimeUnitMS)
                    .readTimeout(pulsarClientOptions.getReadTimeout(), TimeUnitMS)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
