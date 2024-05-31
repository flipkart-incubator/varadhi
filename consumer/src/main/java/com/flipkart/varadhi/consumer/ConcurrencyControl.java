package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface ConcurrencyControl<T> {

    // TODO: maybe evaluate per task enqueue for CC as well.
    Collection<CompletableFuture<T>> enqueueTasks(
            InternalQueueType type, Collection<Supplier<CompletableFuture<T>>> tasks
    );

    void executePendingTasks();
}
