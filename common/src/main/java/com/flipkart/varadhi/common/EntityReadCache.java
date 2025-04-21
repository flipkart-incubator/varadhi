package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.events.EntityEvent;
import com.flipkart.varadhi.common.events.EntityEventListener;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
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
     */
    EntityReadCache(ResourceType resourceType, Supplier<List<T>> entityLoader) {
        this.resourceType = resourceType;
        this.entityLoader = entityLoader;
        this.entities = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new entity cache, and preloads it.
     * <p>
     * This factory method handles the complete lifecycle of cache creation:
     * <ol>
     *   <li>Creates a new cache instance</li>
     *   <li>Preloads the cache with initial data</li>
     * </ol>
     *
     * @param <T> the entity type managed by the cache
     * @param resourceType the type of resource managed by this provider
     * @param entityLoader the strategy for loading entities
     * @return a future that completes with the created and preloaded cache
     * @throws NullPointerException if any parameter is null
     */
    public static <T extends MetaStoreEntity> Future<EntityReadCache<T>> create(
        ResourceType resourceType,
        Supplier<List<T>> entityLoader
    ) {
        Objects.requireNonNull(resourceType, "Resource type cannot be null");
        Objects.requireNonNull(entityLoader, "Entity loader cannot be null");

        EntityReadCache<T> cache = new EntityReadCache<>(resourceType, entityLoader);

        return cache.preload().map(v -> cache);
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
            log.info("Preloading {}", resourceType);
            List<T> entityList = entityLoader.get();

            Map<String, T> entityMap = entityList.stream().collect(Collectors.toMap(T::getName, Function.identity()));

            entities.putAll(entityMap);
            log.info("Preloaded {} {}", entityList.size(), resourceType);
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to preload {}: {}", resourceType, e.getMessage());
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
     * @throws ResourceNotFoundException if the entity is not found
     */
    public T getEntity(String name) {
        Objects.requireNonNull(name, "Entity name cannot be null");
        T entity = entities.get(name);
        if (entity == null) {
            throw new ResourceNotFoundException(String.format("%s(%s) not found", resourceType.name(), name));
        }
        return entity;
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

        if (operation == EventType.UPSERT) {
            T entity = event.resource();
            if (entity != null) {
                entities.put(entityName, entity);
            }
        } else if (operation == EventType.INVALIDATE) {
            entities.remove(entityName);
        }
    }
}
