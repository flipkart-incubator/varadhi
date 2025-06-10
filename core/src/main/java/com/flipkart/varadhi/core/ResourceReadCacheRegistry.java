package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe registry for entity read caches in Varadhi.
 * <p>
 * This registry provides centralized management of {@link ResourceReadCache} instances,
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
public final class ResourceReadCacheRegistry {

    /**
     * Thread-safe storage for entity caches, indexed by resource type.
     */
    private final Map<ResourceType, ResourceReadCache<?>> caches = new ConcurrentHashMap<>();

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
    public <T extends Resource> ResourceReadCacheRegistry register(
        ResourceType type,
        ResourceReadCache<? extends Resource> cache
    ) {
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
    public <T extends Resource> ResourceReadCache<T> getCache(ResourceType type) {
        return Optional.ofNullable(caches.get(type))
                       .map(cache -> (ResourceReadCache<T>)cache)
                       .orElseThrow(() -> new IllegalStateException("No cache registered for resource type: " + type));
    }

    /**
     * Returns a set of all resource types that have registered caches.
     * <p>
     * This method returns a defensive copy of the internal set of resource types,
     * so modifications to the returned set will not affect the registry.
     *
     * @return a set containing all resource types with registered caches
     */
    public Set<ResourceType> getRegisteredResourceTypes() {
        return new HashSet<>(caches.keySet());
    }
}
