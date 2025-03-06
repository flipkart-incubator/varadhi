package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * TODO: lets not put varadhi word in every other class name. VaradhiCache does not tell us its purpose.
 *
 * @param <K>
 * @param <V>
 */
public class VaradhiCache<K, V> {
    // This is NonLoading cache. Get wll invoke loader() on calling thread when needed and hence possibly blocking in
    // some cases.
    // TODO:: need to support unblocking loader call.
    private final Cache<K, V> entityCache;
    private final Function<K, V> entityProvider;
    private final Counter getCounter;
    private final Counter loadCounter;
    private final Counter loadFailureCounter;
    private final BiFunction<K, Throwable, VaradhiException> unExpectedExceptionWrapper;

    // cacheSpec - Guava cache spec as defined at com.google.common.cache.CacheBuilderSpec.
    public VaradhiCache(
        String cacheSpec,
        Function<K, V> entityProvider,
        BiFunction<K, Throwable, VaradhiException> exceptionWrapper,
        String meterPrefix,
        MeterRegistry meterRegistry
    ) {
        this(cacheSpec, Ticker.systemTicker(), entityProvider, exceptionWrapper, meterPrefix, meterRegistry);
    }


    public VaradhiCache(
        String cacheSpec,
        Ticker ticker,
        Function<K, V> entityProvider,
        BiFunction<K, Throwable, VaradhiException> exceptionWrapper,
        String meterPrefix,
        MeterRegistry meterRegistry
    ) {
        this.entityCache = CacheBuilder.from(cacheSpec).ticker(ticker).build();
        this.entityProvider = entityProvider;
        this.unExpectedExceptionWrapper = exceptionWrapper;
        meterRegistry.gauge(getMeterName(meterPrefix, "size"), Tags.empty(), entityCache, Cache::size);
        this.getCounter = meterRegistry.counter(getMeterName(meterPrefix, "gets"), Tags.empty());
        this.loadCounter = meterRegistry.counter(getMeterName(meterPrefix, "loads"), Tags.empty());
        this.loadFailureCounter = meterRegistry.counter(getMeterName(meterPrefix, "loadFailures"), Tags.empty());
    }

    public V get(K key) {
        try {
            getCounter.increment();
            return entityCache.get(key, () -> {
                V value = entityProvider.apply(key);
                loadCounter.increment();
                return value;
            });
        } catch (ExecutionException | UncheckedExecutionException e) {
            loadFailureCounter.increment();
            Throwable realFailure = e.getCause();
            if (realFailure instanceof VaradhiException) {
                throw (VaradhiException)realFailure;
            }
            throw unExpectedExceptionWrapper.apply(key, realFailure);
        }
    }

    private String getMeterName(String meterPrefix, String name) {
        return String.format("varadhi.cache.%s.%s", meterPrefix, name);
    }
}
