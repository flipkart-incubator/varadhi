package com.flipkart.varadhi.events;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import lombok.extern.slf4j.Slf4j;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A lean dispatcher for entity events to appropriate listeners.
 * <p>
 * The dispatcher routes entity events to registered listeners based on resource type,
 * handles error cases gracefully, and provides detailed logging for operational visibility.
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
        log.info("Initialized EntityStateProcessor with supported types: {}", supportedTypes);
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
        if (message == null) {
            return createErrorResponse(null, "Message cannot be null", null);
        }

        String messageId = message.getId();
        EntityEvent<?> rawEvent;
        EntityEvent<T> event;

        try {
            rawEvent = message.getData(EntityEvent.class);
            if (rawEvent == null) {
                return createErrorResponse(messageId, "Invalid event data", null);
            }
        } catch (Exception e) {
            return createErrorResponse(messageId, "Failed to extract event data", e);
        }

        ResourceType resourceType = rawEvent.resourceType();

        if (!supportedTypes.contains(resourceType)) {
            log.debug("Skipping unsupported resource type: {}", resourceType);
            return CompletableFuture.completedFuture(ResponseMessage.fromPayload("Skipped", messageId));
        }

        String resourceName = rawEvent.resourceName();
        log.debug("Processing {} event for {} {}", rawEvent.operation(), resourceType, resourceName);

        try {
            EntityEventListener<T> listener = (EntityEventListener<T>)listeners.get(resourceType);

            Object rawResource = rawEvent.resource();
            T typedResource = null;

            if (rawResource != null) {
                if (rawResource instanceof Map) {
                    String json = JsonMapper.jsonSerialize(rawResource);
                    Class<T> entityClass = getEntityClassForResourceType(resourceType);
                    typedResource = JsonMapper.jsonDeserialize(json, entityClass);
                } else if (rawResource instanceof MetaStoreEntity) {
                    typedResource = (T)rawResource;
                }
            }

            EntityEvent<T> typedEvent = new EntityEvent<>(
                resourceType,
                resourceName,
                rawEvent.operation(),
                typedResource,
                rawEvent.commiter()
            );

            listener.onChange(typedEvent);
            return CompletableFuture.completedFuture(ResponseMessage.fromPayload("OK", messageId));
        } catch (ClassCastException e) {
            String errorMsg = String.format("Resource type mismatch for %s of type %s", resourceName, resourceType);
            log.error(
                "Type mismatch for {} event on resource {}: {}",
                rawEvent.operation(),
                resourceType,
                resourceName,
                e
            );
            return createErrorResponse(messageId, errorMsg, e);
        } catch (Exception e) {
            log.error("Failed to process {} operation for {}: {}", rawEvent.operation(), resourceType, resourceName, e);
            return createErrorResponse(messageId, e.getMessage(), e);
        }
    }

    /**
     * Creates an error response with the given error details.
     * <p>
     * This method handles various error cases and ensures proper logging.
     *
     * @param messageId the ID of the original message, or null if not available
     * @param errorMsg  the error message
     * @param cause     the cause of the error, or null if not available
     * @return a future that completes with the error response
     */
    private CompletableFuture<ResponseMessage> createErrorResponse(String messageId, String errorMsg, Throwable cause) {
        Exception exception = switch (cause) {
            case Exception ex -> ex;
            case null -> new IllegalArgumentException(errorMsg);
            default -> new IllegalArgumentException(errorMsg, cause);
        };

        if (cause == null) {
            log.error(errorMsg);
        } else if (errorMsg.equals(cause.getMessage())) {
            log.error(errorMsg, cause);
        } else {
            log.error("{}: {}", errorMsg, cause.getMessage(), cause);
        }

        return CompletableFuture.completedFuture(ResponseMessage.fromException(exception, messageId));
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

    /**
     * Builder for creating an EntityEventDispatcher with various listeners.
     */
    public static class Builder {

        private final Map<ResourceType, EntityEventListener<?>> listeners = new EnumMap<>(ResourceType.class);

        /**
         * Registers a listener for a specific resource type.
         * <p>
         * If a listener is already registered for the specified resource type,
         * it will be replaced with the new listener.
         *
         * @param resourceType the resource type to register the listener for
         * @param listener     the listener to register
         * @param <T>          the type of resource the listener handles, must extend MetaStoreEntity
         * @throws NullPointerException if resourceType or listener is null
         */
        public <T extends MetaStoreEntity> void withListener(
            ResourceType resourceType,
            EntityEventListener<T> listener
        ) {
            Objects.requireNonNull(resourceType, "Resource type cannot be null");
            Objects.requireNonNull(listener, "Listener cannot be null");
            listeners.put(resourceType, listener);
        }

        /**
         * Builds a new EntityEventDispatcher with the registered listeners.
         *
         * @return a new EntityEventDispatcher
         * @throws IllegalStateException if no listeners are registered
         */
        public EntityEventDispatcher build() {
            if (listeners.isEmpty()) {
                throw new IllegalStateException("At least one listener must be registered");
            }
            return new EntityEventDispatcher(listeners);
        }
    }
}
