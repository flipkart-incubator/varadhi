package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.entities.InternalQueueType;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface ConcurrencyControl<T> {

    // TODO: maybe evaluate per task enqueue for CC as well.
    Collection<CompletableFuture<T>> enqueueTasks(
            InternalQueueType type, Iterable<Supplier<CompletableFuture<T>>> tasks
    );

    /**
     * @return true, if there are free concurrency slots and some tasks can be executed immediately, if enqueued.
     */
    boolean isFree();

    /**
     * A one time task registration, that will be called when there are free concurrency slots. After executing, the task
     * is removed from the registration. Only 1 task can be registered.
     * @param task
     */
    void onFree(Context.Task task);
}
