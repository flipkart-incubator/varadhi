package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.entities.MetaStoreEntity;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A thread-safe, generic cache for entities with event handling capabilities.
 * <p>
 * This class provides concurrent implementation for caching entities with the following features:
 * <ul>
 *   <li>Thread-safe operations using {@link ConcurrentHashMap}</li>
 *   <li>Reactive preloading with {@link Future} for asynchronous operations</li>
 *   <li>Type-safe generic implementation constrained to {@link MetaStoreEntity} types</li>
 * </ul>
 * <p>
 * This class is optimized for concurrent access patterns with a focus on read-heavy workloads,
 * making it suitable where entity data changes infrequently compared to read operations.
 *
 * @param <T> the type of entity managed by this cache, must extend {@link MetaStoreEntity}
 */
@Slf4j
public class EntityReadCache<T extends MetaStoreEntity> implements EntityEventListener<T> {

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
     * The strategy for loading all entities.
     */
    private final Supplier<List<T>> entityLoader;

    /**
     * Creates a new entity cache for the specified resource type with the provided loading strategy.
     *
     * @param resourceType the type of resource managed by this provider
     * @param entityLoader the strategy for loading all entities
     * @throws NullPointerException if resourceType or entityTypeName is null
     */
    public EntityReadCache(ResourceType resourceType, Supplier<List<T>> entityLoader) {
        this.resourceType = Objects.requireNonNull(resourceType, "Resource type cannot be null");
        this.entityLoader = Objects.requireNonNull(entityLoader, "Entity loader cannot be null");
        this.entities = new ConcurrentHashMap<>();
        log.debug("Initialized {} provider", resourceType);
    }

    /**
     * Preloads all entities into the in-memory cache.
     * <p>
     * This method fetches all entities using the configured entity loader and
     * populates the cache. It is thread-safe and can be called concurrently.
     * <p>
     * The operation is performed asynchronously and returns a {@link Future} that
     * completes when the preload operation is finished.
     *
     * @return a future that completes when the preload operation is finished
     */
    public Future<Void> preload() {
        Promise<Void> promise = Promise.promise();

        try {
            log.info("Starting to preload {}", resourceType.toString());
            List<T> entityList = entityLoader.get();

            if (entityList.isEmpty()) {
                log.info("No {} found to preload", resourceType);
                promise.complete();
                return promise.future();
            }

            Map<String, T> entityMap = entityList.stream().collect(Collectors.toMap(T::getName, Function.identity()));

            entities.putAll(entityMap);
            log.info("Successfully preloaded {} {}", entityList.size(), resourceType);
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to preload {}", resourceType.toString(), e);
            promise.fail(e);
        }

        return promise.future();
    }

    /**
     * Returns the entity with the given name.
     *
     * @param name the name of the entity to get
     * @return the entity with the given name, or null if not found
     * @throws NullPointerException if name is null
     */
    public T getEntity(String name) {
        Objects.requireNonNull(name, "Entity name cannot be null");
        return entities.get(name);
    }

    /**
     * Handles entity change events by updating the in-memory cache.
     * <p>
     * This method is thread-safe due to the underlying ConcurrentHashMap.
     * It processes events based on their operation type:
     * <ul>
     *   <li>{@link EventType#UPSERT}: Adds or updates an entity in the cache</li>
     *   <li>{@link EventType#INVALIDATE}: Removes an entity from the cache</li>
     * </ul>
     *
     * @param event the entity event to handle
     */
    @Override
    public void onChange(EntityEvent<? extends T> event) {
        String entityName = event.resourceName();
        EventType operation = event.operation();

        switch (operation) {
            case UPSERT -> {
                T entity = event.resource();
                if (entity != null) {
                    entities.put(entityName, entity);
                    log.debug("Updated {} in local cache: {}", resourceType.toString(), entityName);
                } else {
                    log.warn(
                        "Received UPSERT event with null resource for {}: {}",
                        resourceType.toString(),
                        entityName
                    );
                }
            }
            case INVALIDATE -> {
                T removed = entities.remove(entityName);
                if (removed != null) {
                    log.debug("Removed {} from local cache: {}", resourceType.toString(), entityName);
                } else {
                    log.warn("Attempted to remove non-existent {} from cache: {}", resourceType.toString(), entityName);
                }
            }
            default -> log.warn("Unsupported operation {} for {}: {}", operation, resourceType.toString(), entityName);
        }
    }
}
