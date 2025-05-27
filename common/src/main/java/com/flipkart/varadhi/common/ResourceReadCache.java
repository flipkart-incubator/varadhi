package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.events.ResourceEvent;
import com.flipkart.varadhi.common.events.ResourceEventListener;
import com.flipkart.varadhi.common.events.EventType;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A thread-safe, generic cache for resource with event handling capabilities.
 * <p>
 * This class provides concurrent implementation for caching resource with the following features:
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
public class ResourceReadCache<T extends Resource> implements ResourceEventListener<T> {

    /**
     * Thread-safe storage for resource, indexed by their names.
     */
    protected final ConcurrentHashMap<String, T> resource;

    /**
     * The resource type managed by this provider.
     */
    @Getter
    protected final ResourceType resourceType;

    /**
     * The strategy for loading all resource.
     */
    private final Supplier<List<T>> entityLoader;

    /**
     * Creates a new entity cache for the specified resource type with the provided loading strategy.
     *
     * @param resourceType the type of resource managed by this provider
     * @param resourceLoader the strategy for loading all resource
     */
    ResourceReadCache(ResourceType resourceType, Supplier<List<T>> resourceLoader) {
        this.resourceType = resourceType;
        this.entityLoader = resourceLoader;
        this.resource = new ConcurrentHashMap<>();
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
     * @param entityLoader the strategy for loading resource
     * @param vertx the Vert.x instance to use for non-blocking operations
     * @return a future that completes with the created and preloaded cache
     * @throws NullPointerException if any parameter is null
     */
    public static <T extends Resource> Future<ResourceReadCache<T>> create(
        ResourceType resourceType,
        Supplier<List<T>> entityLoader,
        Vertx vertx
    ) {
        Objects.requireNonNull(resourceType, "Resource type cannot be null");
        Objects.requireNonNull(entityLoader, "Entity loader cannot be null");
        Objects.requireNonNull(vertx, "Vertx instance cannot be null");

        ResourceReadCache<T> cache = new ResourceReadCache<>(resourceType, entityLoader);

        return cache.preload(vertx).map(v -> cache);
    }

    /**
     * Preloads all resource into the in-memory cache.
     * <p>
     * This method fetches all resource using the configured entity loader and
     * populates the cache. It is thread-safe and can be called concurrently.
     * <p>
     * The operation is performed asynchronously and returns a {@link Future} that
     * completes when the preload operation is finished.
     * <p>
     * This implementation uses Vert.x's executeBlocking to avoid blocking the event loop
     * during the potentially expensive entity loading operation.
     *
     * @param vertx the Vert.x instance to use for non-blocking operations
     * @return a future that completes when the preload operation is finished
     */
    Future<Void> preload(Vertx vertx) {
        log.info("Preloading {}", resourceType);

        return vertx.executeBlocking(entityLoader::get, false).<Void>map(entityList -> {
            Map<String, T> entityMap = entityList.stream().collect(Collectors.toMap(T::getName, Function.identity()));
            resource.putAll(entityMap);
            log.info("Preloaded {} {}", entityList.size(), resourceType);
            return null;
        }).onFailure(e -> log.error("Failed to preload {}: {}", resourceType, e.getMessage()));
    }

    /**
     * Returns the entity with the given name.
     *
     * @param name the name of the entity to get. Must not be null.
     * @return an Optional containing the entity with the given name, or empty if not found
     */
    public Optional<T> get(String name) {
        return Optional.ofNullable(resource.get(name));
    }

    /**
     * Returns the entity with the given name.
     *
     * @param name the name of the entity to get. Must not be null.
     * @return the entity with the given name.
     * @throws ResourceNotFoundException if the entity is not found
     */
    public T getOrThrow(String name) {
        T entity = resource.get(name);
        if (entity == null) {
            throw new ResourceNotFoundException("%s(%s) not found".formatted(resourceType.name(), name));
        }
        return entity;
    }

    /**
     * Handles entity change events by updating the in-memory cache.
     * <p>
     * This method is thread-safe due to the underlying ConcurrentHashMap.
     * It processes events based on their operation type:
     * <ul>
     *   <li>{@link EventType#UPSERT}: Adds or updates an entity in the cache if the event version
     *      is greater than the current cached entity's version</li>
     *   <li>{@link EventType#INVALIDATE}: Removes an entity from the cache</li>
     * </ul>
     *
     * @param event the entity event to handle
     */
    @Override
    public void onChange(ResourceEvent<? extends T> event) {
        String entityName = event.resourceName();
        EventType operation = event.operation();

        if (operation == EventType.UPSERT) {
            T entity = event.resource();
            if (entity != null) {
                resource.compute(entityName, (key, existingEntity) -> {
                    if (existingEntity == null || event.version() > existingEntity.getVersion()) {
                        return entity;
                    }
                    return existingEntity;
                });
            }
        } else if (operation == EventType.INVALIDATE) {
            resource.remove(entityName);
        }
    }
}
