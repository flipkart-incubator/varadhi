package com.flipkart.varadhi.common;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;

public class TestHelper {
    public static <T> void assertListEquals(List<T> expected, List<T> actual) {
        assertEquals(expected.size(), actual.size());
        assertTrue(expected.containsAll(actual));
    }

    public static <T extends Exception, V> T assertException(
        CompletableFuture<V> future,
        Class<T> expectedExceptionClazz,
        String expectedFailureMsg
    ) {
        T actual = assertThrows(expectedExceptionClazz, () -> {
            try {
                future.get();
            } catch (ExecutionException ee) {
                throw ee.getCause();
            }
        });
        if (null != expectedFailureMsg) {
            assertEquals(expectedFailureMsg, actual.getMessage());
        }
        return actual;
    }

    public static <T> void assertValue(T expectedValue, CompletableFuture<T> future) {
        try {
            assertEquals(expectedValue, future.get());
        } catch (Exception e) {
            Assertions.fail("Failed to get result from future.", e);
        }
    }

    public static <T> T awaitAsyncAndGetValue(CompletableFuture<T> future) {
        try {
            await().atMost(100, TimeUnit.SECONDS).until(future::isDone);
            return future.get();
        } catch (Exception e) {
            Assertions.fail("Failed to get result from future.", e);
            throw new RuntimeException("Test should have failed.");
        }
    }
}
