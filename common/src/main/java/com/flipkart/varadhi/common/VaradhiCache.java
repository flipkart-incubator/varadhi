package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A thread-safe generic cache implementation with metrics support.
 * This cache provides basic operations like get, put, invalidate with metrics tracking
 * for hits, misses, and size.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
@Slf4j
public final class VaradhiCache<K, V> {
    private static final String METRIC_NAME_SIZE = "varadhi.cache.size";
    private static final String METRIC_NAME_OPERATIONS = "varadhi.cache.operations";
    private static final String TAG_CACHE_NAME = "entity";
    private static final String TAG_OPERATION = "operation";
    private static final String OPERATION_HIT = "hit";
    private static final String OPERATION_MISS = "miss";

    private final ConcurrentMap<K, V> cache;
    private final String cacheName;
    private final Counter hitCounter;
    private final Counter missCounter;

    /**
     * Constructs a new cache with the specified name and metrics registry.
     *
     * @param cacheName     the name of the cache, used for metrics reporting
     * @param meterRegistry the registry for recording metrics
     * @throws NullPointerException if cacheName or meterRegistry is null
     */
    public VaradhiCache(String cacheName, MeterRegistry meterRegistry) {
        this.cache = new ConcurrentHashMap<>();
        this.cacheName = Objects.requireNonNull(cacheName, "Cache name cannot be null");
        Objects.requireNonNull(meterRegistry, "MeterRegistry cannot be null");

        Tags cacheTags = Tags.of(TAG_CACHE_NAME, cacheName);

        // Register size metric with cache name tag
        meterRegistry.gauge(
                METRIC_NAME_SIZE,
                cacheTags,
                cache,
                ConcurrentMap::size
        );

        // Register operation counters with appropriate tags
        this.hitCounter = meterRegistry.counter(
                METRIC_NAME_OPERATIONS,
                cacheTags.and(TAG_OPERATION, OPERATION_HIT)
        );
        this.missCounter = meterRegistry.counter(
                METRIC_NAME_OPERATIONS,
                cacheTags.and(TAG_OPERATION, OPERATION_MISS)
        );

        log.debug("Initialized cache: {}", cacheName);
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with the specified key, or null if no mapping exists
     * @throws NullPointerException if the specified key is null
     */
    public V get(K key) {
        Objects.requireNonNull(key, "Key cannot be null");
        V value = cache.get(key);
        if (value != null) {
            hitCounter.increment();
            log.trace("Cache hit for key: {}", key);
        } else {
            missCounter.increment();
            log.trace("Cache miss for key: {}", key);
        }
        return value;
    }

    /**
     * Associates the specified value with the specified key in this cache.
     *
     * @param key   the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     * @throws NullPointerException if the specified key or value is null
     */
    public void put(K key, V value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");
        cache.put(key, value);
        log.trace("Added entry for key: {}", key);
    }

    /**
     * Removes the entry for the specified key if present.
     *
     * @param key the key whose mapping is to be removed
     * @throws NullPointerException if the specified key is null
     */
    public void invalidate(K key) {
        Objects.requireNonNull(key, "Key cannot be null");
        cache.remove(key);
        log.trace("Invalidated entry for key: {}", key);
    }

    /**
     * Removes all entries from this cache.
     */
    public void invalidateAll() {
        cache.clear();
        log.debug("Invalidated all entries in cache: {}", cacheName);
    }
}
