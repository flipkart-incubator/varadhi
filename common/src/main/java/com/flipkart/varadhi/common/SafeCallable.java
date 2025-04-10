package com.flipkart.varadhi.common;

@FunctionalInterface
public interface SafeCallable<T> {
    T call();
}
