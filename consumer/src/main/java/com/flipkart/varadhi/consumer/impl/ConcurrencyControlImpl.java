package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.CircularQueue;
import com.flipkart.varadhi.consumer.ConcurrencyControl;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.entities.InternalQueueType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Not supposed to be thread safe. It is expected that tasks will be enqueued & dequeued from the same thread which
 * is tied to the context.
 */
public class ConcurrencyControlImpl<T> implements ConcurrencyControl<T> {

    private final Context context;

    @Getter
    private final int maxConcurrency;

    private final AtomicInteger concurrency = new AtomicInteger(0);

    private final TaskQueue<T>[] queues;

    private final AtomicInteger pendingTasks = new AtomicInteger(0);

    private final AtomicInteger schedulePendingTaskCounter = new AtomicInteger(0);

    /**
     * @param maxConcurrency
     * @param priorityOrder  The order in which the queues should be processed
     */
    public ConcurrencyControlImpl(Context context, int maxConcurrency, InternalQueueType[] priorityOrder) {
        this.context = context;
        this.maxConcurrency = maxConcurrency;
        this.queues = new TaskQueue[priorityOrder.length];

        for (int i = 0; i < priorityOrder.length; i++) {
            this.queues[i] = new TaskQueue<>(priorityOrder[i]);
        }
    }

    @Override
    public Collection<CompletableFuture<T>> enqueueTasks(
            InternalQueueType type, Iterable<Supplier<CompletableFuture<T>>> tasks
    ) {
        int currentConcurrency = executePendingTasksInternal();

        List<CompletableFuture<T>> futures = new ArrayList<>();
        Iterator<Supplier<CompletableFuture<T>>> tasksIt = tasks.iterator();

        // these tasks, we can directly launch
        while (currentConcurrency < maxConcurrency && tasksIt.hasNext()) {
            Supplier<CompletableFuture<T>> task = tasksIt.next();
            futures.add(task.get().whenComplete(this::onTaskCompletion));
            currentConcurrency = concurrency.incrementAndGet();
        }

        if (tasksIt.hasNext()) {
            // add all tasks to the queue
            TaskQueue<T> queue = getQueue(type);

            while (tasksIt.hasNext()) {
                Supplier<CompletableFuture<T>> task = tasksIt.next();
                CompletableFuture<T> future = new CompletableFuture<>();
                queue.tasks.add(new Holder<>(future, task, this::onTaskCompletion));
                futures.add(future);
            }
        }

        return futures;
    }

    public void executePendingTasks() {
        executePendingTasksInternal();
    }

    /**
     * @return currentConcurrency
     */
    int executePendingTasksInternal() {
        int currentConcurrency = concurrency.get();

        assert currentConcurrency <= maxConcurrency;

        if (currentConcurrency == maxConcurrency) {
            return currentConcurrency;
        }

        // go through all the queued tasks in priority order and execute them
        for (TaskQueue<T> queue : queues) {
            while (currentConcurrency < maxConcurrency && !queue.tasks.isEmpty()) {
                Holder<T> taskHolder = queue.tasks.poll();
                currentConcurrency = concurrency.incrementAndGet();
                pendingTasks.decrementAndGet();
                taskHolder.execute();
            }
        }

        return currentConcurrency;
    }

    private TaskQueue<T> getQueue(InternalQueueType type) {
        for (TaskQueue<T> queue : queues) {
            if (queue.type == type) {
                return queue;
            }
        }
        throw new IllegalStateException("Queue not found for type: " + type);
    }

    public int getPendingCount() {
        return pendingTasks.get();
    }

    /**
     * Computation to be run when the enqueued task finishes its execution.
     *
     * 1. Free up the concurrency slot, so that this slot can be used by pending tasks.
     * 2. If this was the "last" concurrency slot, then we should enqueue a task to schedule any pending tasks.
     * Otherwise, pending tasks may sit idle forever.
     * 3. If there is no task running at the moment to schedule any pending task, then we should schedule it regardless.
     *
     * This method can run on any arbitrary thread.
     *
     * @param result
     * @param ex
     */
    private void onTaskCompletion(T result, Throwable ex) {
        int newConcurrency = concurrency.decrementAndGet();

        boolean scheduleRequired = false;
        if (schedulePendingTaskCounter.compareAndSet(0, 1)) {
            scheduleRequired = true;
        } else if (newConcurrency == 0) {
            schedulePendingTaskCounter.incrementAndGet();
            scheduleRequired = true;
        }

        if (scheduleRequired) {
            context.runOnContext(() -> {
                executePendingTasks();
                schedulePendingTaskCounter.decrementAndGet();
            });
        }
    }

    @RequiredArgsConstructor
    static class TaskQueue<T> {
        private final InternalQueueType type;
        private final CircularQueue<Holder<T>> tasks = new CircularQueue<>(16);
    }

    static class Holder<T> {
        private final CompletableFuture<T> future;
        private final Supplier<CompletableFuture<T>> task;
        private final BiConsumer<T, Throwable> onComplete;

        public Holder(
                CompletableFuture<T> future, Supplier<CompletableFuture<T>> task, BiConsumer<T, Throwable> onComplete
        ) {
            this.future = future;
            this.task = task;
            this.onComplete = onComplete;
        }

        public void execute() {
            CompletableFuture<T> taskFuture = task.get();
            taskFuture.whenComplete((result, error) -> {
                onComplete.accept(result, error);
                if (error != null) {
                    future.completeExceptionally(error);
                } else {
                    future.complete(result);
                }
            });
        }
    }
}
