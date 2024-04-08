package com.flipkart.varadhi.consumer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface ConcurrencyControl<T> {
    Collection<CompletableFuture<T>> enqueueTasks(InternalQueueType type, Collection<Supplier<CompletableFuture<T>>> tasks);

    void executePendingTasks();
}
