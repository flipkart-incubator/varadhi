package com.flipkart.varadhi.events;

import com.flipkart.varadhi.common.VaradhiCache;
import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.core.cluster.EventListener;
import com.flipkart.varadhi.entities.InternalCompositeTopic;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.spi.services.Producer;
import lombok.extern.slf4j.Slf4j;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Processes entity state changes and updates in-memory state accordingly.
 * <p>
 * This processor uses a functional approach to manage entity state updates for different resource types,
 * providing a clean and extensible way to handle entity events across the system.
 */
@Slf4j
public final class EntityStateProcessor implements EventListener {

    /**
     * Functional interface for handling different event types on a specific resource.
     *
     * @param <T> the type of resource being handled
     */
    @FunctionalInterface
    private interface EntityEventProcessor<T> {

        /**
         * Processes an event for a specific resource.
         *
         * @param eventType    the type of event (UPSERT, INVALIDATE)
         * @param resourceName the name of the resource
         * @param resource     the resource object (maybe null for INVALIDATE events)
         */
        void process(EventType eventType, String resourceName, T resource);
    }

    private final Map<ResourceType, EntityEventProcessor<?>> processors = new EnumMap<>(ResourceType.class);
    private final Set<ResourceType> supportedTypes;

    /**
     * Creates a new EntityStateProcessor with the specified services.
     *
     * @param projectService   the project service (maybe null if project events are not needed)
     * @param producerService  the producer service (maybe null if topic events are not needed)
     * @param produceRegion    the region for producing messages
     * @param producerProvider the function to create producers for storage topics
     */
    public EntityStateProcessor(
        ProjectService projectService,
        ProducerService producerService,
        String produceRegion,
        Function<StorageTopic, Producer> producerProvider
    ) {

        // Register project state processor if project service is provided
        if (projectService != null) {
            registerProjectProcessor(projectService);
        }

        // Register topic state processor if producer service is provided
        if (producerService != null) {
            registerTopicProcessor(producerService, produceRegion, producerProvider);
        }

        this.supportedTypes = Set.copyOf(processors.keySet());
        log.info("Initialized EntityStateProcessor with supported types: {}", supportedTypes);
    }

    private void registerProjectProcessor(ProjectService projectService) {
        VaradhiCache<String, Project> projectCache = projectService.getProjectCache();
        Objects.requireNonNull(projectCache, "Project cache cannot be null");

        processors.put(ResourceType.PROJECT, createEventProcessor(
                // UPSERT processor
                (resourceName, resource) -> {
                    Project project = (Project) resource;
                    projectCache.put(resourceName, project);
                    log.info("Updated project state for: {}", resourceName);
                },
                // INVALIDATE processor
                resourceName -> {
                    projectCache.invalidate(resourceName);
                    log.info("Removed project state for: {}", resourceName);
                    return null;
                }
        ));
    }

    private void registerTopicProcessor(
            ProducerService producerService,
            String produceRegion,
            Function<StorageTopic, Producer> producerProvider) {

        VaradhiCache<StorageTopic, Producer> producerCache =
                Objects.requireNonNull(producerService.getProducerCache(), "Producer cache cannot be null");
        VaradhiCache<String, VaradhiTopic> topicCache =
                Objects.requireNonNull(producerService.getInternalTopicCache(), "Topic cache cannot be null");

        processors.put(ResourceType.TOPIC, createEventProcessor(
                // UPSERT handler
                (resourceName, resource) -> {
                    VaradhiTopic topic = (VaradhiTopic) resource;
                    topicCache.put(resourceName, topic);

                    InternalCompositeTopic internalTopic = topic.getProduceTopicForRegion(produceRegion);
                    if (internalTopic != null && internalTopic.getTopicState().isProduceAllowed()) {
                        StorageTopic storageTopic = internalTopic.getTopicToProduce();
                        Producer producer = producerProvider.apply(storageTopic);
                        producerCache.put(storageTopic, producer);
                        log.debug("Created and cached producer for storage topic: {}", storageTopic.getName());
                    }
                    log.info("Updated topic state for: {}", resourceName);
                },
                // INVALIDATE handler
                resourceName -> {
                    VaradhiTopic existingTopic = topicCache.get(resourceName);
                    if (existingTopic != null) {
                        InternalCompositeTopic internalTopic = existingTopic.getProduceTopicForRegion(produceRegion);
                        if (internalTopic != null) {
                            producerCache.invalidate(internalTopic.getTopicToProduce());
                            log.debug("Removed producer for storage topic");
                        }
                    }
                    topicCache.invalidate(resourceName);
                    log.info("Removed topic state for: {}", resourceName);
                    return null;
                }
        ));
    }

    /**
     * Creates an event processor with separate functions for UPSERT and INVALIDATE operations.
     *
     * @param upsertProcessor     the function to handle UPSERT events
     * @param invalidateProcessor the function to handle INVALIDATE events
     * @param <T>                 the type of resource being handled
     * @return an event processor that delegates to the appropriate function based on event type
     */
    private <T> EntityEventProcessor<T> createEventProcessor(
            BiConsumer<String, T> upsertProcessor,
            Function<String, Void> invalidateProcessor) {
        return (eventType, resourceName, resource) -> {
            try {
                switch (eventType) {
                    case UPSERT -> {
                        if (resource == null) {
                            log.warn("Received UPSERT event with null resource for {}", resourceName);
                            return;
                        }
                        upsertProcessor.accept(resourceName, resource);
                    }
                    case INVALIDATE -> {
                        invalidateProcessor.apply(resourceName);
                    }
                    default -> log.warn("Unsupported operation {} for resource: {}", eventType, resourceName);
                }
            } catch (ClassCastException e) {
                log.error("Type mismatch for {} event on resource: {}", eventType, resourceName, e);
                throw new IllegalArgumentException("Resource type mismatch for " + resourceName, e);
            } catch (Exception e) {
                log.error("Error handling {} event for resource: {}", eventType, resourceName, e);
                throw e;
            }
        };
    }

    @Override
    public void processEvent(EntityEvent<?> event) {
        Objects.requireNonNull(event, "Event cannot be null");

        ResourceType resourceType = event.resourceType();
        if (!supportedTypes.contains(resourceType)) {
            log.debug("Skipping unsupported resource type: {}", resourceType);
            return;
        }

        String resourceName = event.resourceName();
        log.debug("Processing {} event for {} {}", event.operation(), resourceType, resourceName);

        try {
            @SuppressWarnings ("unchecked")
            EntityEventProcessor<Object> handler = (EntityEventProcessor<Object>) processors.get(resourceType);
            handler.process(event.operation(), resourceName, event.resource());
        } catch (ClassCastException e) {
            log.error("Type mismatch for {} operation on {}: {}. Expected resource type does not match actual type.",
                    event.operation(), resourceType, resourceName, e);
            throw new IllegalArgumentException("Resource type mismatch for " + resourceType + ": " + resourceName, e);
        } catch (Exception e) {
            log.error("Failed to process {} operation for {}: {}", event.operation(), resourceType, resourceName, e);
            throw e;
        }
    }
}
