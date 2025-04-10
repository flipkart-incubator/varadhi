package com.flipkart.varadhi.common;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.exceptions.VaradhiException;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import lombok.EqualsAndHashCode;
import lombok.experimental.StandardException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class VaradhiCacheTests {

    VaradhiCache<String, DummyData> testCache;
    MeterRegistry meterRegistry;
    Counter hitCounter;
    Counter missCounter;
    Counter loadCounter;
    Gauge cacheSize;
    VaradhiCacheTests entityProvider;

    @BeforeEach
    void PreTest() {
        entityProvider = spy(this);
        meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        testCache = new VaradhiCache<>("test", meterRegistry);

        Tags cacheTags = Tags.of("entity", "test");
        hitCounter = meterRegistry.counter("varadhi.cache.operations", cacheTags.and("operation", "hit"));
        missCounter = meterRegistry.counter("varadhi.cache.operations", cacheTags.and("operation", "miss"));
        loadCounter = meterRegistry.counter("varadhi.cache.operations", cacheTags.and("operation", "load"));
        cacheSize = meterRegistry.find("varadhi.cache.size").tags(cacheTags).gauge();
    }

    private void validateCounters(int hit, int miss, int load, int size) {
        Assertions.assertEquals(hit, (int)hitCounter.count());
        Assertions.assertEquals(miss, (int)missCounter.count());
        Assertions.assertEquals(load, (int)loadCounter.count());
        Assertions.assertEquals(size, (int)cacheSize.value());
    }

    @Test
    void testGetData() {
        DummyData data1 = testCache.getOrCompute("key1", entityProvider::getData);
        Assertions.assertEquals(1, (int)cacheSize.value());

        DummyData data2 = testCache.getOrCompute("key2", entityProvider::getData);
        Assertions.assertEquals(2, (int)cacheSize.value());

        Assertions.assertEquals(data1, testCache.getOrCompute("key1", entityProvider::getData));
        Assertions.assertEquals(data2, testCache.getOrCompute("key2", entityProvider::getData));
        Assertions.assertEquals(data1, testCache.getOrCompute("key1", entityProvider::getData));
        Assertions.assertEquals(data2, testCache.getOrCompute("key2", entityProvider::getData));

        validateCounters(4, 2, 2, 2);

        verify(entityProvider, times(1)).getData("key1");
        verify(entityProvider, times(1)).getData("key2");
    }

    @Test
    void testGetDataWithException() {
        doThrow(new ResourceNotFoundException("Resource not found.")).when(entityProvider).getData("key1");

        ResourceNotFoundException re = Assertions.assertThrows(
            ResourceNotFoundException.class,
            () -> testCache.getOrCompute("key1", entityProvider::getData)
        );

        Assertions.assertEquals("Resource not found.", re.getMessage());

        validateCounters(0, 1, 0, 0);
    }

    @Test
    void testGetDataWithOtherException() {
        doThrow(new CustomException("Load failure.")).when(entityProvider).getData("key1");

        VaradhiException ce = Assertions.assertThrows(
            VaradhiException.class,
            () -> testCache.getOrCompute("key1", entityProvider::getData)
        );

        Assertions.assertEquals("Error finding resource: key1", ce.getMessage());

        validateCounters(0, 1, 0, 0);
    }

    @Test
    void testBasicOperations() {
        DummyData data1 = new DummyData("key1");
        testCache.put("key1", data1);

        DummyData retrieved = testCache.get("key1");
        Assertions.assertEquals(data1, retrieved);

        validateCounters(1, 0, 0, 1);

        testCache.invalidate("key1");
        Assertions.assertNull(testCache.get("key1"));

        validateCounters(1, 1, 0, 0);
    }

    @Test
    void testPutAll() {
        java.util.Map<String, DummyData> items = new java.util.HashMap<>();
        items.put("key1", new DummyData("key1"));
        items.put("key2", new DummyData("key2"));

        testCache.putAll(items);

        validateCounters(0, 0, 2, 2);

        Assertions.assertEquals(items.get("key1"), testCache.get("key1"));
        Assertions.assertEquals(items.get("key2"), testCache.get("key2"));

        validateCounters(2, 0, 2, 2);
    }

    @Test
    void testInvalidateAll() {
        testCache.put("key1", new DummyData("key1"));
        testCache.put("key2", new DummyData("key2"));

        Assertions.assertEquals(2, testCache.size());

        testCache.invalidateAll();

        Assertions.assertEquals(0, testCache.size());
        Assertions.assertFalse(testCache.containsKey("key1"));
        Assertions.assertFalse(testCache.containsKey("key2"));
    }

    DummyData getData(String key) {
        return new DummyData(key);
    }

    @EqualsAndHashCode
    public static class DummyData {
        String key;
        long value;

        public DummyData(String key) {
            this.key = key;
            this.value = System.nanoTime();
        }
    }


    @StandardException
    public static class CustomException extends VaradhiException {
    }
}
