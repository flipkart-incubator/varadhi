package com.flipkart.varadhi.common.utils;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Helpers for unwrapping async exception chains.
 */
public final class ThrowableUtils {

    public static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            Throwable cause = t.getCause();
            return cause != null ? cause : t;
        }
        return t;
    }

    public static String rootMessage(Throwable t) {
        Throwable cause = unwrap(t);
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }
}
