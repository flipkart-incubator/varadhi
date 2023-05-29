package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.StorageTopicFactory;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarTopicFactory;
import com.flipkart.varadhi.pulsar.services.PulsarTopicServiceFactory;
import com.flipkart.varadhi.services.PlatformOptions;
import com.flipkart.varadhi.services.PlatformProvider;
import com.flipkart.varadhi.services.StorageTopicServiceFactory;
import com.flipkart.varadhi.utils.YamlLoader;


public class PulsarProvider implements PlatformProvider {
    private PulsarTopicServiceFactory pulsarTopicServiceFactory;
    private PulsarTopicFactory pulsarTopicFactory;
    private volatile boolean initialised = false;
    public void init(PlatformOptions platformOptions) {
        if (!initialised) {
            synchronized (this) {
                if (!initialised) {
                    PulsarConfig pulsarConfig = YamlLoader.loadConfig(platformOptions.getConfigFile(), PulsarConfig.class);
                    pulsarTopicFactory = new PulsarTopicFactory();
                    pulsarTopicServiceFactory = new PulsarTopicServiceFactory(pulsarConfig.getPulsarClientOptions());
                    initialised = true;
                }
            }
        }
    }

    public <T extends StorageTopic> StorageTopicFactory<T> getStorageTopicFactory() {
        if (!initialised) {
            throw new InvalidStateException("PlatformProvider is not yet initialised.");
        }
        return (StorageTopicFactory) pulsarTopicFactory;
    }

    public <T extends StorageTopic>  StorageTopicServiceFactory<T> getStorageTopicServiceFactory() {
        if (!initialised) {
            throw new InvalidStateException("PlatformProvider is not yet initialised.");
        }
        return (StorageTopicServiceFactory) pulsarTopicServiceFactory ;
    }
}
