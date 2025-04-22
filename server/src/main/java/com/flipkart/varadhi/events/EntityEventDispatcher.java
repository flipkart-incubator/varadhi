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
import com.flipkart.varadhi.common.EntityReadCacheRegistry;
import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.common.Constants.ENTITY_EVENTS_HANDLER;

/**
 * A lean dispatcher for entity events to appropriate listeners.
 * <p>
 * The dispatcher routes entity events to registered listeners based on resource type,
 * handles error cases gracefully.
 *
 * @see EntityEvent
 * @see EntityEventListener
 * @see ResourceType
 */
@Slf4j
public final class EntityEventDispatcher {

    /**
     * Map of resource types to their corresponding event listeners.
     * This map is immutable after construction for thread safety.
     */
    private final Map<ResourceType, EntityEventListener<?>> listeners;

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
    private EntityEventDispatcher(Map<ResourceType, EntityEventListener<?>> listeners) {
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
    public <T extends MetaStoreEntity> CompletableFuture<ResponseMessage> processEvent(ClusterMessage message) {
        String messageId = message.getId();

        try {
            EntityEvent<?> rawEvent = message.getData(EntityEvent.class);
            ResourceType resourceType = rawEvent.resourceType();

            if (!supportedTypes.contains(resourceType)) {
                return CompletableFuture.completedFuture(ResponseMessage.fromPayload("Skipped", messageId));
            }

            EntityEventListener<T> listener = (EntityEventListener<T>)listeners.get(resourceType);
            T typedResource = convertResource(rawEvent.resource(), resourceType);

            EntityEvent<T> typedEvent = new EntityEvent<>(
                resourceType,
                rawEvent.resourceName(),
                rawEvent.operation(),
                typedResource,
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
    private <T extends MetaStoreEntity> T convertResource(Object rawResource, ResourceType resourceType) {
        if (rawResource instanceof Map) {
            String json = JsonMapper.jsonSerialize(rawResource);
            return JsonMapper.jsonDeserialize(json, getEntityClassForResourceType(resourceType));
        }
        return (T)rawResource;
    }

    /**
     * Returns the entity class for the given resource type.
     * This method maps ResourceType enum values to their corresponding entity classes.
     *
     * @param resourceType the resource type
     * @return the entity class for the resource type
     * @throws IllegalArgumentException if the resource type is not supported
     */
    @SuppressWarnings ("unchecked")
    private <T extends MetaStoreEntity> Class<T> getEntityClassForResourceType(ResourceType resourceType) {
        return switch (resourceType) {
            case PROJECT -> (Class<T>)Project.class;
            case TOPIC -> (Class<T>)VaradhiTopic.class;
            default -> throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        };
    }

    public static void bindToClusterEntityEvents(
        Vertx vertx,
        MemberInfo memberInfo,
        VaradhiClusterManager clusterManager,
        EntityReadCacheRegistry cacheRegistry
    ) {
        String hostname = memberInfo.hostname();
        EntityEventDispatcher dispatcher = build(cacheRegistry);
        int listenersAdded = dispatcher.supportedTypes.size();

        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        messageRouter.requestHandler(hostname, ENTITY_EVENTS_HANDLER, dispatcher::processEvent);

        log.info("Entity event handlers initialized with {} listeners", listenersAdded);
    }

    static EntityEventDispatcher build(EntityReadCacheRegistry cacheRegistry) {
        return new EntityEventDispatcher(
            cacheRegistry.getRegisteredResourceTypes()
                         .stream()
                         .collect(Collectors.toMap(Function.identity(), cacheRegistry::getCache))
        );
    }
}
