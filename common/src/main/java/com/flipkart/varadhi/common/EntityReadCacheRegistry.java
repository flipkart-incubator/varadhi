package com.flipkart.varadhi.common;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.auth.ResourceType;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A thread-safe registry for entity read caches in Varadhi.
 * <p>
 * This registry provides centralized management of {@link EntityReadCache} instances,
 * allowing for:
 * <ul>
 *   <li>Dynamic registration and retrieval of caches by resource type</li>
 *   <li>Type-safe access to entity caches</li>
 *   <li>Parallel preloading of all registered caches</li>
 * </ul>
 * <p>
 * The registry is designed for concurrent access in a multithreaded environment,
 * using {@link ConcurrentHashMap} for thread-safe operations without explicit locking.
 */
@Slf4j
public final class EntityReadCacheRegistry {

    /**
     * Thread-safe storage for entity caches, indexed by resource type.
     */
    private final Map<ResourceType, EntityReadCache<?>> caches;

    /**
     * Creates a new empty registry with a thread-safe concurrent map.
     */
    public EntityReadCacheRegistry() {
        this.caches = new ConcurrentHashMap<>();
        log.debug("Initialized EntityReadCacheRegistry");
    }

    /**
     * Registers an entity cache for the specified resource type.
     * <p>
     * If a cache for the specified type already exists, it will be replaced.
     *
     * @param <T>   the entity type managed by the cache
     * @param type  the resource type to register the cache for
     * @param cache the cache instance to register
     * @return this registry instance for method chaining
     * @throws NullPointerException if type or cache is null
     */
    public <T extends MetaStoreEntity> EntityReadCacheRegistry register(ResourceType type, EntityReadCache<T> cache) {
        Objects.requireNonNull(type, "Resource type cannot be null");
        Objects.requireNonNull(cache, "Cache cannot be null");

        caches.put(type, cache);
        log.info("Registered entity cache for resource type: {}", type);
        return this;
    }

    /**
     * Creates and registers a new entity cache for the specified resource type.
     * <p>
     * This is a convenience method that creates a new {@link EntityReadCache} instance
     * and registers it with this registry.
     *
     * @param <T>          the entity type managed by the cache
     * @param type         the resource type to register the cache for
     * @param entityLoader the supplier for loading entities
     * @return this registry instance for method chaining
     * @throws NullPointerException if any parameter is null
     */
    public <T extends MetaStoreEntity> EntityReadCacheRegistry register(
        ResourceType type,
        Supplier<List<T>> entityLoader
    ) {
        Objects.requireNonNull(type, "Resource type cannot be null");
        Objects.requireNonNull(entityLoader, "Entity loader cannot be null");

        EntityReadCache<T> cache = new EntityReadCache<>(type, entityLoader);
        return register(type, cache);
    }

    /**
     * Gets the cache for the specified resource type.
     *
     * @param <T>  the entity type managed by the cache
     * @param type the resource type to get the cache for
     * @return the cache for the specified type
     * @throws IllegalStateException if no cache is registered for the specified type
     * @throws NullPointerException  if type is null
     */
    @SuppressWarnings ("unchecked")
    public <T extends MetaStoreEntity> EntityReadCache<T> getCache(ResourceType type) {
        Objects.requireNonNull(type, "Resource type cannot be null");

        return Optional.ofNullable(caches.get(type))
                       .map(cache -> (EntityReadCache<T>)cache)
                       .orElseThrow(() -> new IllegalStateException("No cache registered for resource type: " + type));
    }

    /**
     * Preloads all registered caches in parallel.
     * <p>
     * This method initiates preloading for all registered caches concurrently and
     * returns a future that completes when all preloading operations are finished.
     * If any cache fails to preload, the returned future will be failed with the first
     * encountered exception.
     *
     * @return a future that completes when all caches are preloaded
     */
    public Future<Void> preloadAll() {
        if (caches.isEmpty()) {
            log.warn("No caches registered for preloading");
            return Future.succeededFuture();
        }

        log.info("Starting preload of {} registered caches", caches.size());
        Promise<Void> promise = Promise.promise();

        List<Future<Void>> futures = caches.entrySet()
                                           .stream()
                                           .map(
                                               entry -> entry.getValue()
                                                             .preload()
                                                             .onSuccess(
                                                                 v -> log.debug(
                                                                     "Preloaded cache for {}",
                                                                     entry.getKey()
                                                                 )
                                                             )
                                                             .onFailure(
                                                                 e -> log.error(
                                                                     "Failed to preload cache for {}: {}",
                                                                     entry.getKey(),
                                                                     e.getMessage(),
                                                                     e
                                                                 )
                                                             )
                                           )
                                           .toList();

        Future.all(futures).onSuccess(v -> {
            log.info("Successfully preloaded all {} caches", caches.size());
            promise.complete();
        }).onFailure(e -> {
            log.error("Failed to preload all caches: {}", e.getMessage(), e);
            promise.fail(e);
        });

        return promise.future();
    }
}
