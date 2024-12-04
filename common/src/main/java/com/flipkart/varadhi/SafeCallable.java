package com.flipkart.varadhi;

@FunctionalInterface
public interface SafeCallable<T> {
    T call();
}
