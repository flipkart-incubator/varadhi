package com.flipkart.varadhi.events;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.flipkart.varadhi.cluster.MessageRouter;
import com.flipkart.varadhi.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.common.ResourceReadCacheRegistry;
import com.flipkart.varadhi.common.events.ResourceEvent;
import com.flipkart.varadhi.common.events.ResourceEventListener;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.EntityType;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.common.Constants.ENTITY_EVENTS_HANDLER;

/**
 * A lean dispatcher for entity events to appropriate listeners.
 * <p>
 * The dispatcher routes entity events to registered listeners based on resource type,
 * handles error cases gracefully.
 *
 * @see ResourceEvent
 * @see ResourceEventListener
 * @see EntityType
 */
@Slf4j
public final class EntityEventDispatcher {

    /**
     * Map of resource types to their corresponding event listeners.
     * This map is immutable after construction for thread safety.
     */
    private final Map<ResourceType, ResourceEventListener<?>> listeners;

    /**
     * Set of resource types supported by this dispatcher.
     * This set is derived from the listeners map and is immutable.
     */
    private final Set<ResourceType> supportedTypes;

    /**
     * Creates a new EntityEventDispatcher with the specified listeners.
     *
     * @param listeners a map of resource types to their corresponding event listeners
     */
    private EntityEventDispatcher(Map<ResourceType, ResourceEventListener<?>> listeners) {
        this.listeners = Map.copyOf(listeners);
        this.supportedTypes = Set.copyOf(this.listeners.keySet());
    }

    /**
     * Processes an entity event from a cluster message.
     * <p>
     * This method extracts the entity event from the message, validates it,
     * and routes it to the appropriate listener based on the resource type.
     *
     * @param message the cluster message containing the entity event
     * @param <T>     the type of entity in the event, must extend MetaStoreEntity
     * @return a future that completes with the response message
     */
    @SuppressWarnings ("unchecked")
    public <T extends Resource> CompletableFuture<ResponseMessage> processEvent(ClusterMessage message) {
        String messageId = message.getId();

        try {
            ResourceEvent<?> rawEvent = message.getData(ResourceEvent.class);
            ResourceType entityType = rawEvent.resourceType();

            if (!supportedTypes.contains(entityType)) {
                return CompletableFuture.completedFuture(ResponseMessage.fromPayload("Skipped", messageId));
            }

            ResourceEventListener<T> listener = (ResourceEventListener<T>)listeners.get(entityType);
            T typedResource = convertResource(rawEvent.resource(), entityType);

            ResourceEvent<T> typedEvent = new ResourceEvent<>(
                entityType,
                rawEvent.resourceName(),
                rawEvent.operation(),
                typedResource,
                rawEvent.version(),
                rawEvent.commiter()
            );

            listener.onChange(typedEvent);
            return CompletableFuture.completedFuture(ResponseMessage.fromPayload("OK", messageId));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(ResponseMessage.fromException(e, messageId));
        }
    }

    /**
     * Converts a raw resource object to the appropriate entity type.
     *
     * @param rawResource  the raw resource object
     * @param resourceType the type of resource
     * @param <T>          the entity type
     * @return the converted entity
     * @throws IllegalArgumentException if the resource cannot be converted
     */
    @SuppressWarnings ("unchecked")
    private <T extends Resource> T convertResource(Object rawResource, ResourceType resourceType) {
        if (rawResource instanceof Map) {
            String json = JsonMapper.jsonSerialize(rawResource);
            return JsonMapper.jsonDeserialize(json, getEntityClassForResourceType(resourceType));
        }
        return (T)rawResource;
    }

    /**
     * Returns the resource class for the given resource type.
     * This method maps EntityType enum values to their corresponding entity classes.
     *
     * @param resourceType the resource type
     * @return the entity class for the resource type
     * @throws IllegalArgumentException if the resource type is not supported
     */
    @SuppressWarnings ("unchecked")
    private <T extends Resource> Class<T> getEntityClassForResourceType(ResourceType resourceType) {
        return switch (resourceType) {
            case PROJECT, TOPIC -> (Class<T>)Resource.EntityResource.class;
            default -> throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        };
    }

    /**
     * Binds an EntityEventDispatcher to cluster entity events.
     * <p>
     * This method creates an EntityEventDispatcher from the provided cache registry and
     * registers it with the cluster's message router to handle entity events. It uses
     * the member's hostname to route events to the appropriate handler.
     * <p>
     * The dispatcher will handle events for all resource types registered in the cache registry.
     *
     * @param vertx          the Vert.x instance for handling asynchronous operations
     * @param memberInfo     information about the current cluster member
     * @param clusterManager the cluster manager for inter-node communication
     * @param cacheRegistry  the registry containing entity caches that will act as event listeners
     * @throws NullPointerException if any parameter is null
     */
    public static void bindToClusterEntityEvents(
        Vertx vertx,
        MemberInfo memberInfo,
        VaradhiClusterManager clusterManager,
        ResourceReadCacheRegistry cacheRegistry
    ) {
        String hostname = memberInfo.hostname();
        EntityEventDispatcher dispatcher = build(cacheRegistry);
        int listenersAdded = dispatcher.supportedTypes.size();

        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        messageRouter.requestHandler(hostname, ENTITY_EVENTS_HANDLER, dispatcher::processEvent);

        log.info("Entity event handlers initialized with {} listeners", listenersAdded);
    }

    /**
     * Builds an EntityEventDispatcher from the provided cache registry.
     * <p>
     * This method creates a new EntityEventDispatcher that will use the entity caches
     * in the registry as event listeners. It maps each registered resource type to its
     * corresponding cache, which implements the ResourceEventListener interface.
     *
     * @param cacheRegistry the registry containing entity caches that will act as event listeners
     * @return a new EntityEventDispatcher configured with listeners from the cache registry
     * @throws NullPointerException if cacheRegistry is null
     */
    static EntityEventDispatcher build(ResourceReadCacheRegistry cacheRegistry) {
        return new EntityEventDispatcher(
            cacheRegistry.getRegisteredResourceTypes()
                         .stream()
                         .collect(Collectors.toMap(Function.identity(), cacheRegistry::getCache))
        );
    }
}
