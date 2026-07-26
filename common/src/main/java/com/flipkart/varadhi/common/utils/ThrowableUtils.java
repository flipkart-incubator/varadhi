package com.flipkart.varadhi.common.utils;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Helpers for unwrapping async exception chains.
 */
public final class ThrowableUtils {

    /**
     * Unwraps one level of {@link CompletionException} or {@link ExecutionException}.
     * Other throwables are returned as-is.
     */
    public static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            Throwable cause = t.getCause();
            return cause != null ? cause : t;
        }
        return t;
    }

    /**
     * Returns {@link Throwable#getMessage()} for the unwrapped throwable, or the simple class name when absent.
     */
    public static String rootMessage(Throwable t) {
        Throwable cause = unwrap(t);
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }
}
