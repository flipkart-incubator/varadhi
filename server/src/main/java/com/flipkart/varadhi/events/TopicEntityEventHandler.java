package com.flipkart.varadhi.events;

import com.flipkart.varadhi.VaradhiCache;
import com.flipkart.varadhi.core.cluster.EntityEventHandler;
import com.flipkart.varadhi.entities.EntityEvent;
import com.flipkart.varadhi.entities.InternalCompositeTopic;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.spi.services.Producer;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

@Slf4j
public final class TopicEntityEventHandler implements EntityEventHandler {
    private static final Set<ResourceType> SUPPORTED_TYPES = Collections.singleton(ResourceType.TOPIC);

    private final VaradhiCache<StorageTopic, Producer> producerCache;
    private final VaradhiCache<String, VaradhiTopic> topicCache;
    private final String produceRegion;
    private final Function<StorageTopic, Producer> producerProvider;

    public TopicEntityEventHandler(
            ProducerService producerService,
            String produceRegion,
            Function<StorageTopic, Producer> producerProvider) {
        this.producerCache = Objects.requireNonNull(producerService.getProducerCache(), "Producer cache cannot be null");
        this.topicCache = Objects.requireNonNull(producerService.getInternalTopicCache(), "Topic cache cannot be null");
        this.produceRegion = produceRegion;
        this.producerProvider = producerProvider;
    }

    @Override
    public void handleEvent(EntityEvent event) {
        if (event.resourceType() != ResourceType.TOPIC) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping non-topic event type: {}", event.resourceType());
            }
            return;
        }

        String topicName = event.resourceName();
        try {
            switch (event.operation()) {
                case UPSERT -> handleUpsert(topicName, (VaradhiTopic) event.resourceState());
                case INVALIDATE -> handleInvalidate(topicName);
                default -> log.warn("Unsupported operation {} for topic: {}", event.operation(), topicName);
            }
        } catch (Exception e) {
            log.error("Failed to process {} operation for topic: {}", event.operation(), topicName, e);
            throw e;
        }
    }

    @Override
    public Set<ResourceType> getSupportedResourceTypes() {
        return SUPPORTED_TYPES;
    }

    private void handleUpsert(String topicName, VaradhiTopic topic) {
        Objects.requireNonNull(topic, "Topic state cannot be null for UPSERT operation");
        topicCache.put(topicName, topic);

        InternalCompositeTopic internalTopic = topic.getProduceTopicForRegion(produceRegion);
        if (internalTopic != null && internalTopic.getTopicState().isProduceAllowed()) {
            StorageTopic storageTopic = internalTopic.getTopicToProduce();
            Producer producer = producerProvider.apply(storageTopic);
            producerCache.put(storageTopic, producer);
        }
        log.info("Updated cache for topic: {}", topicName);
    }

    private void handleInvalidate(String topicName) {
        VaradhiTopic existingTopic = topicCache.get(topicName);
        if (existingTopic != null) {
            InternalCompositeTopic internalTopic = existingTopic.getProduceTopicForRegion(produceRegion);
            if (internalTopic != null) {
                producerCache.invalidate(internalTopic.getTopicToProduce());
            }
        }
        topicCache.invalidate(topicName);
        log.info("Removed topic and producer from cache: {}", topicName);
    }
}
