package com.flipkart.varadhi.common;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.entities.auth.ResourceType;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe registry for entity read caches in Varadhi.
 * <p>
 * This registry provides centralized management of {@link EntityReadCache} instances,
 * allowing for:
 * <ul>
 *   <li>Dynamic registration and retrieval of caches by resource type</li>
 *   <li>Type-safe access to entity caches</li>
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
    private final Map<ResourceType, EntityReadCache<?>> caches = new ConcurrentHashMap<>();

    /**
     * Registers an entity cache for the specified resource type.
     * <p>
     * This method assumes that the cache is already preloaded and hooked into appropriate
     * event listeners. If a cache for the specified type already exists, an exception is thrown.
     *
     * @param <T>   the entity type managed by the cache
     * @param type  the resource type to register the cache for
     * @param cache the cache instance to register
     * @return this registry instance for method chaining
     * @throws IllegalStateException if a cache is already registered for the specified type
     */
    public <T extends MetaStoreEntity> EntityReadCacheRegistry register(ResourceType type, EntityReadCache<T> cache) {
        if (caches.containsKey(type)) {
            throw new IllegalStateException("Cache already registered for resource type: " + type);
        }

        caches.put(type, cache);
        log.info("Registered entity cache for resource type: {}", type);
        return this;
    }

    /**
     * Gets the cache for the specified resource type.
     *
     * @param <T>  the entity type managed by the cache
     * @param type the resource type to get the cache for
     * @return the cache for the specified type
     * @throws IllegalStateException if no cache is registered for the specified type
     */
    @SuppressWarnings ("unchecked")
    public <T extends MetaStoreEntity> EntityReadCache<T> getCache(ResourceType type) {
        return Optional.ofNullable(caches.get(type))
                       .map(cache -> (EntityReadCache<T>)cache)
                       .orElseThrow(() -> new IllegalStateException("No cache registered for resource type: " + type));
    }

    public Set<ResourceType> getRegisteredResourceTypes() {
        return new HashSet<>(caches.keySet());
    }
}
