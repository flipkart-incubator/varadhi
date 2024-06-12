package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface Throttler<T> {
    CompletableFuture<T> acquire(InternalQueueType type, Supplier<CompletableFuture<T>> task, int permits);
}
