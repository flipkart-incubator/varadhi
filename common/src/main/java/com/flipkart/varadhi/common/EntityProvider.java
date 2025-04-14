package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.entities.auth.ResourceType;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A thread-safe, generic provider for entity management with event handling capabilities.
 * <p>
 * This abstract class provides a foundation for implementing entity providers that need to:
 * <ul>
 *   <li>Maintain an in-memory cache of entities</li>
 *   <li>Handle entity change events</li>
 * </ul>
 * <p>
 * Implementations must provide entity loading and naming strategies by implementing
 * the abstract methods {@link #loadAllEntities()} and {@link #getEntityName(Object)}.
 * <p>
 * This class is optimized for concurrent access patterns with a focus on read-heavy workloads,
 * using a {@link ConcurrentHashMap} for storage which provides thread-safety for individual operations.
 *
 * @param <T> the type of entity managed by this provider
 */
@Slf4j
public abstract class EntityProvider<T> implements EntityEventListener<T> {

    /**
     * Thread-safe storage for entities, indexed by their names.
     */
    protected final ConcurrentHashMap<String, T> entities;

    /**
     * The resource type managed by this provider.
     */
    @Getter
    protected final ResourceType resourceType;

    /**
     * A human-readable name for the entity type, used in logging and error messages.
     */
    protected final String entityTypeName;

    /**
     * Creates a new entity provider for the specified resource type.
     *
     * @param resourceType   the type of resource managed by this provider
     * @param entityTypeName a human-readable name for the entity type
     * @throws NullPointerException if resourceType or entityTypeName is null
     */
    protected EntityProvider(ResourceType resourceType, String entityTypeName) {
        this.resourceType = Objects.requireNonNull(resourceType, "Resource type cannot be null");
        this.entityTypeName = Objects.requireNonNull(entityTypeName, "Entity type name cannot be null");
        this.entities = new ConcurrentHashMap<>();
        log.debug("Initialized {} provider", entityTypeName);
    }

    /**
     * Loads all entities from the underlying data source.
     * <p>
     * Implementations must provide a concrete loading strategy.
     *
     * @return a list of all entities
     */
    protected abstract List<T> loadAllEntities();

    /**
     * Returns the name of the given entity.
     * <p>
     * Implementations must provide a concrete naming strategy.
     *
     * @param entity the entity to get the name for
     * @return the name of the entity
     */
    protected abstract String getEntityName(T entity);

    /**
     * Preloads all entities into the in-memory cache.
     * <p>
     * This method is thread-safe due to the underlying ConcurrentHashMap.
     *
     * @return a future that completes when the preload operation is finished
     */
    public Future<Void> preload() {
        Promise<Void> promise = Promise.promise();

        try {
            log.info("Starting to preload {}", entityTypeName);
            List<T> entityList = loadAllEntities();

            if (entityList.isEmpty()) {
                log.info("No {} found to preload", entityTypeName);
                promise.complete();
                return promise.future();
            }

            Map<String, T> entityMap = entityList.stream()
                                                 .collect(Collectors.toMap(this::getEntityName, Function.identity()));

            entities.putAll(entityMap);
            log.info("Successfully preloaded {} {}", entityList.size(), entityTypeName);
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to preload {}", entityTypeName, e);
            promise.fail(e);
        }

        return promise.future();
    }

    /**
     * Returns the entity with the given name.
     *
     * @param name the name of the entity to get
     * @return the entity with the given name, or null if not found
     */
    public T getEntity(String name) {
        return entities.get(name);
    }

    /**
     * Handles entity change events by updating the in-memory cache.
     * <p>
     * This method is thread-safe due to the underlying ConcurrentHashMap.
     *
     * @param event the entity event to handle
     */
    @Override
    public void onChange(EntityEvent<T> event) {
        String entityName = event.resourceName();
        EventType operation = event.operation();

        switch (operation) {
            case UPSERT -> {
                T entity = event.resource();
                if (entity != null) {
                    entities.put(entityName, entity);
                    log.debug("Updated {} in local cache: {}", entityTypeName, entityName);
                } else {
                    log.warn("Received UPSERT event with null resource for {}: {}", entityTypeName, entityName);
                }
            }
            case INVALIDATE -> {
                T removed = entities.remove(entityName);
                if (removed != null) {
                    log.debug("Removed {} from local cache: {}", entityTypeName, entityName);
                } else {
                    log.debug("Attempted to remove non-existent {} from cache: {}", entityTypeName, entityName);
                }
            }
            default -> log.warn("Unsupported operation {} for {}: {}", operation, entityTypeName, entityName);
        }
    }
}
