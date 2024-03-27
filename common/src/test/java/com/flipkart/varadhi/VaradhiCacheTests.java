package com.flipkart.varadhi;

import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.google.common.base.Ticker;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import lombok.EqualsAndHashCode;
import lombok.experimental.StandardException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class VaradhiCacheTests {

    VaradhiCache<String, DummyData> testCache;
    MeterRegistry meterRegistry;
    Counter getCounter;
    Counter loadCounter;
    Counter loadFailureCounter;
    Gauge cacheSize;
    VaradhiCacheTests entityProvider;
    DummyTicker ticker;

    @BeforeEach
    public void PreTest() {
        ticker = new DummyTicker(System.nanoTime());
        String cacheSpec = "expireAfterWrite=3600s";
        entityProvider = spy(this);
        meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        testCache =
                new VaradhiCache<>(cacheSpec, ticker, entityProvider::getData, (key, failure) -> new CustomException(
                        String.format("Failed to get data (%s): %s", key, failure.getMessage()), failure),
                        "test", meterRegistry
                );
        getCounter = meterRegistry.counter("varadhi.cache.test.gets");
        loadCounter = meterRegistry.counter("varadhi.cache.test.loads");
        cacheSize = meterRegistry.find("varadhi.cache.test.size").gauge();
        loadFailureCounter = meterRegistry.counter("varadhi.cache.test.loadFailures");
    }

    private void validateCounters(int get, int load, int loadFailures, int size) {
        Assertions.assertEquals(get, (int) getCounter.count());
        Assertions.assertEquals(load, (int) loadCounter.count());
        Assertions.assertEquals(loadFailures, (int) loadFailureCounter.count());
        Assertions.assertEquals(size, (int) cacheSize.value());
    }

    @Test
    public void testGetData() {
        DummyData data1 = testCache.get("key1");
        Assertions.assertEquals(1, (int) cacheSize.value());
        DummyData data2 = testCache.get("key2");
        Assertions.assertEquals(data1, testCache.get("key1"));
        Assertions.assertEquals(data2, testCache.get("key2"));
        Assertions.assertEquals(data1, testCache.get("key1"));
        Assertions.assertEquals(data2, testCache.get("key2"));
        validateCounters(6, 2, 0, 2);

        verify(entityProvider, times(1)).getData("key1");
        verify(entityProvider, times(1)).getData("key2");
    }

    @Test
    public void testGetDataKnownException() {
        doThrow(new ResourceNotFoundException("Data not found.")).when(entityProvider).getData("key1");
        ResourceNotFoundException re =
                Assertions.assertThrows(ResourceNotFoundException.class, () -> testCache.get("key1"));
        Assertions.assertEquals("Data not found.", re.getMessage());
        validateCounters(1, 0, 1, 0);
    }

    @Test
    public void testGetDataUnKnownException() {
        doThrow(new RuntimeException("Load failure.")).when(entityProvider).getData("key1");
        CustomException ce =
                Assertions.assertThrows(CustomException.class, () -> testCache.get("key1"));
        Assertions.assertEquals("Failed to get data (key1): Load failure.", ce.getMessage());
        Throwable realFailure = ce.getCause();
        Assertions.assertEquals("Load failure.", realFailure.getMessage());
        validateCounters(1, 0, 1, 0);
    }

    @Test
    public void testDataExpiry() throws InterruptedException {
        DummyData data1 = testCache.get("key1");
        ticker.advance(3601);
        DummyData data2 = testCache.get("key1");
        validateCounters(2, 2, 0, 1);
        verify(entityProvider, times(2)).getData("key1");
        Assertions.assertNotEquals(data1, data2);
    }

    @Test
    public void testEntryRemovalOnDataExpiryWithLoaderException() throws InterruptedException {
        testCache.get("key1");
        validateCounters(1, 1, 0, 1);
        ticker.advance(3601);
        Thread.sleep(1); // to ensure new version of DummyData().
        doThrow(new ResourceNotFoundException("Data not found.")).when(entityProvider).getData("key1");
        ResourceNotFoundException re =
                Assertions.assertThrows(ResourceNotFoundException.class, () -> testCache.get("key1"));
        validateCounters(2, 1, 1, 0);
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

    public static class DummyTicker extends Ticker {
        long value;

        public DummyTicker(long value) {
            this.value = value;
        }

        public void advance(long seconds) {
            value += seconds * 1000 * 1000 * 1000;
        }

        @Override
        public long read() {
            return value;
        }
    }
}
