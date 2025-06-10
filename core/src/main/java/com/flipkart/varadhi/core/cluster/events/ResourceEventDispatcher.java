package com.flipkart.varadhi.core.cluster.events;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.flipkart.varadhi.core.ResourceReadCacheRegistry;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.common.Constants.ENTITY_EVENTS_HANDLER;

/**
 * A lean dispatcher for entity events to appropriate listeners.
 * Dispatches Cluster Messages to appropriate ResourceEventListeners.
 * <p>
 * The dispatcher routes entity events to registered listeners based on resource type,
 * handles error cases gracefully.
 *
 * @see ResourceEvent
 * @see ResourceEventListener
 * @see ResourceType
 */
@Slf4j
public final class ResourceEventDispatcher {

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
     * Creates a new ResourceEventDispatcher with the specified listeners.
     *
     * @param listeners a map of resource types to their corresponding event listeners
     */
    private ResourceEventDispatcher(Map<ResourceType, ResourceEventListener<?>> listeners) {
        this.listeners = Map.copyOf(listeners);
        this.supportedTypes = Set.copyOf(this.listeners.keySet());
    }

    /**
     * Processes resource event from a cluster message.
     * <p>
     * This method extracts the resource event from the message, validates it,
     * and routes it to the appropriate listener based on the resource type.
     *
     * @param message the cluster message containing the resource event
     * @param <T>     the type of resource in the event, must extend Resource
     * @return a future that completes with the response message
     */
    @SuppressWarnings ("unchecked")
    public <T extends Resource> CompletableFuture<ResponseMessage> processEvent(ClusterMessage message) {
        String messageId = message.getId();

        try {
            ResourceEvent<?> rawEvent = message.getData(ResourceEvent.class);
            ResourceType resourceType = rawEvent.resourceType();

            if (!supportedTypes.contains(resourceType)) {
                return CompletableFuture.completedFuture(ResponseMessage.fromPayload("Skipped", messageId));
            }

            ResourceEventListener<T> listener = (ResourceEventListener<T>)listeners.get(resourceType);
            T typedResource = convertResource(rawEvent.resource(), resourceType);

            ResourceEvent<T> typedEvent = new ResourceEvent<>(
                resourceType,
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
            return JsonMapper.jsonDeserialize(json, getResourceClass(resourceType));
        }
        return (T)rawResource;
    }

    /**
     * Returns the resource class for the given resource type.
     * This method maps ResourceType enum values to their corresponding entity classes.
     *
     * @param resourceType the resource type
     * @return the entity class for the resource type
     * @throws IllegalArgumentException if the resource type is not supported
     */
    @SuppressWarnings ("unchecked")
    private <T extends Resource> Class<T> getResourceClass(ResourceType resourceType) {
        return switch (resourceType) {
            default -> (Class<T>)Resource.EntityResource.class;
        };
    }

    /**
     * Binds an ResourceEventDispatcher to cluster entity events.
     * <p>
     * This method creates an ResourceEventDispatcher from the provided cache registry and
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
        ResourceEventDispatcher dispatcher = build(cacheRegistry);
        int listenersAdded = dispatcher.supportedTypes.size();

        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        messageRouter.requestHandler(hostname, ENTITY_EVENTS_HANDLER, dispatcher::processEvent);

        log.info("Entity event handlers initialized with {} listeners", listenersAdded);
    }

    /**
     * Builds an ResourceEventDispatcher from the provided cache registry.
     * <p>
     * This method creates a new ResourceEventDispatcher that will use the entity caches
     * in the registry as event listeners. It maps each registered resource type to its
     * corresponding cache, which implements the ResourceEventListener interface.
     *
     * @param cacheRegistry the registry containing entity caches that will act as event listeners
     * @return a new ResourceEventDispatcher configured with listeners from the cache registry
     * @throws NullPointerException if cacheRegistry is null
     */
    static ResourceEventDispatcher build(ResourceReadCacheRegistry cacheRegistry) {
        return new ResourceEventDispatcher(
            cacheRegistry.getRegisteredResourceTypes()
                         .stream()
                         .collect(Collectors.toMap(Function.identity(), cacheRegistry::getCache))
        );
    }
}
