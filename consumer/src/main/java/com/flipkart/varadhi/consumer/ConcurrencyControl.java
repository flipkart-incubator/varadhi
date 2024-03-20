package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.CircularQueue;
import com.flipkart.varadhi.consumer.concurrent.Context;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Not supposed to be thread safe. It is expected that tasks will be enqueued & dequeued from the same thread which
 * is tied to the context.
 */
public class ConcurrencyControl<T> {

    private final Context context;

    @Getter
    private final int maxConcurrency;

    private final AtomicInteger concurrency = new AtomicInteger(0);

    private final TaskQueue<T>[] queues;

    private final AtomicBoolean wakeUpEnqueued = new AtomicBoolean(false);

    /**
     * @param maxConcurrency
     * @param priorityOrder  The order in which the queues should be processed
     */
    public ConcurrencyControl(Context context, int maxConcurrency, InternalQueueType[] priorityOrder) {
        this.context = context;
        this.maxConcurrency = maxConcurrency;
        this.queues = new TaskQueue[priorityOrder.length];

        for (int i = 0; i < priorityOrder.length; i++) {
            this.queues[i] = new TaskQueue<>(priorityOrder[i]);
        }
    }

    public Collection<CompletableFuture<T>> enqueue(
            InternalQueueType type, Collection<Supplier<CompletableFuture<T>>> tasks
    ) {
        int freeConcurrency = maxConcurrency - concurrency.get();

        // go through all the queued tasks in priority order and execute them
        for (TaskQueue<T> queue : queues) {
            while (freeConcurrency > 0 && !queue.tasks.isEmpty()) {
                Holder<T> taskHolder = queue.tasks.poll();
                taskHolder.execute();
                freeConcurrency--;
                concurrency.incrementAndGet();
            }
        }

        List<CompletableFuture<T>> futures = new ArrayList<>();
        Iterator<Supplier<CompletableFuture<T>>> tasksIt = tasks.iterator();

        // these tasks, we can directly launch
        while (freeConcurrency > 0 && tasksIt.hasNext()) {
            Supplier<CompletableFuture<T>> task = tasksIt.next();
            futures.add(task.get().whenComplete(this::onComplete));
            freeConcurrency--;
            concurrency.incrementAndGet();
        }

        if (freeConcurrency <= 0) {
            // add all tasks to the queue
            TaskQueue<T> queue = getQueue(type);

            for (Supplier<CompletableFuture<T>> task : tasks) {
                CompletableFuture<T> future = new CompletableFuture<>();
                queue.tasks.add(new Holder<>(future, task, this::onComplete));
                futures.add(future);
            }
        }

        return futures;
    }

    private TaskQueue<T> getQueue(InternalQueueType type) {
        for (TaskQueue<T> queue : queues) {
            if (queue.type == type) {
                return queue;
            }
        }
        throw new IllegalStateException("Queue not found for type: " + type);
    }

    int getPendingCount() {
        int count = 0;
        for (TaskQueue<T> queue : queues) {
            count += queue.tasks.size();
        }
        return count;
    }

    void onComplete(T result, Throwable ex) {
        concurrency.decrementAndGet();
        if (wakeUpEnqueued.compareAndSet(false, true)) {
            context.getExecutor().execute(() -> {
                enqueue(queues[0].type, Collections.emptyList());
                wakeUpEnqueued.set(false);
            });
        }

        // TODO: there is a corner case here. If this method finishes while not taking care of all pending tasks, and
        // concurrency reduced to 0, then there wont be no-one to execute this wakeUp call.
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
