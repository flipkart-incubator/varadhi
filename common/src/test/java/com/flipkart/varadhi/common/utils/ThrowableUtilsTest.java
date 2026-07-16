package com.flipkart.varadhi.common.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ThrowableUtilsTest {

    @Test
    void unwrap_returnsCauseForCompletionException() {
        IllegalStateException root = new IllegalStateException("boom");
        assertSame(root, ThrowableUtils.unwrap(new CompletionException(root)));
    }

    @Test
    void unwrap_returnsCauseForExecutionException() {
        IllegalStateException root = new IllegalStateException("boom");
        assertSame(root, ThrowableUtils.unwrap(new ExecutionException(root)));
    }

    @Test
    void rootMessage_fallsBackToClassNameWhenMessageMissing() {
        assertEquals("IllegalStateException", ThrowableUtils.rootMessage(new IllegalStateException()));
    }
}
