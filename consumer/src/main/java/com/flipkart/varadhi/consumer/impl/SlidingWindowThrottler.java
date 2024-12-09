package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.consumer.ThresholdProvider;
import com.flipkart.varadhi.consumer.Throttler;
import com.google.common.base.Ticker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Supplier;

/**
 * Looks similar to CC. implement it and see if they share structure.
 *
 * @param <T>
 */
@Slf4j
public class SlidingWindowThrottler<T>
        implements Throttler<T>, ThresholdProvider.ThresholdChangeListener, AutoCloseable {

    /*
        Approach:
        - Overall window size for defining the rate limit. Is it rate per sec / per min. Here the rate is per window size. But it technically does not
          dictate whether the rate should be consumed / replenished uniformly over the window. Usually it is not.
          Do we require this uniformity? Preferably yes. Lets aim for it, as it is ideal in definition. This behaviour is also easier to test.
          But not technically needed. So lets keep the solution efficient and performant.
        - Tick rate / sub-interval: smaller than window size. If it smaller, than it allows us to approximately track the usage of permits across the whole window at this sub-interval level. And then we use this info, to replinish the permits.
          Basically, the previous utilization will roughly dictate how the permits are going to be replinised.
        - Starting config: window_size = 1 sec, sub-interval = 10ms.


        what is required?
        - A thread to automatically manage execution whenever permits are "freed".
        - A list of pending tasks per type.
        - Task executor is either the enqueuer / scheduler thread. Why can't the completer, same doubt for CC? Exploration
     */

    private final ScheduledExecutorService scheduler;
    private final Ticker ticker;
    private final long tickMs;
    private final int ticksInWindow;
    private final int totalTicks;
    private volatile float threshold;

    /**
     * The ticks array is used to store the number of data points in each tick. This array acts as circular queue.
     * Can be updated by arbitrary threads.
     */
    private final AtomicIntegerArray permitsPerTick;
    private int permitsConsumedInLastWindow;
    private volatile long windowBeginTick;

    private final TaskQueue<T>[] queues;

    /**
     * Guards the "only" execution of pending tasks. If the execution is being attempted then this flag will be true, and
     * no one else should attempt parallel execution.
     */
    private final AtomicBoolean executionInProgress = new AtomicBoolean(false);

    private ScheduledFuture<?> throttledTaskExecutor;

    public SlidingWindowThrottler(
            ScheduledExecutorService scheduler, Ticker ticker, float initialThreshold, int windowSizeMs, int tickMs,
            InternalQueueType[] priorityOrder
    ) {
        this.scheduler = scheduler;
        this.ticker = ticker;
        this.threshold = initialThreshold;
        this.tickMs = tickMs;

        if (windowSizeMs % tickMs != 0) {
            throw new IllegalArgumentException("Window size should be a multiple of tick rate");
        }
        this.ticksInWindow = windowSizeMs / tickMs;

        // We are using 2 times, so that we can track 2 window worth of datapoints.
        this.totalTicks = 2 * ticksInWindow;
        this.permitsPerTick = new AtomicIntegerArray(totalTicks);
        this.permitsConsumedInLastWindow = 0;
        this.queues = new TaskQueue[priorityOrder.length];
        for (int i = 0; i < priorityOrder.length; i++) {
            this.queues[i] = new TaskQueue<>(priorityOrder[i]);
        }
        this.windowBeginTick = (ticker.read() / 1_000_000) / tickMs - ticksInWindow;
        this.throttledTaskExecutor = scheduleTask();
    }

    @Override
    public CompletableFuture<T> acquire(InternalQueueType type, Supplier<CompletableFuture<T>> task, int permits) {

        // TODO: evaluate if we should call executePendingTasksInternal()?
        CompletableFuture<T> future = new CompletableFuture<>();
        Holder<T> holder = new Holder<>(future, task, permits);
        getQueue(type).tasks.add(holder);
        return future;
    }

    @Override
    public void onThresholdChange(float newThresholdPerSec) {
        // scale the threshold for the window.
        this.threshold = newThresholdPerSec * (tickMs * ticksInWindow / 1000.0f);
    }

    @Override
    public void close() throws Exception {
        if (throttledTaskExecutor != null) {
            throttledTaskExecutor.cancel(true);
        }
    }

    /**
     * If there is another thread trying execution, then simply return.
     */
    void executePendingTasksInternal() {
        if (executionInProgress.compareAndExchange(false, true)) {
            // previous value was already true, so another thread is already executing the pending tasks.
            return;
        }

        long now = ticker.read() / 1_000_000;
        long currentTick = now / tickMs;

        moveWindow(currentTick);

        try {
            // go over the task queue in priority order, and execute the tasks.
            // A task is executed only if we have the permits of that task available.
            // Permits available is computed using the difference between the threshold & permits given out in the last window.
            // If permit is larger than the threshold, then we simply wait for multiple ticks to pass, before we can execute the task. On each tick we decrement the permit required for that task, based on how many permits are available.
            // If the permit is less than the threshold, then we execute the task immediately & add datapoints in the ticks array to denote the permits consumed.

            // if remaining permits is <= 0, then return without doing anything.
            float permitsConsumed = getPermitsConsumedPrecise(now, currentTick);
            float freePermits = threshold - permitsConsumed;
            if (freePermits <= 0) {

                return;
            }

            // execute the tasks in priority order.
            for (TaskQueue<T> queue : queues) {
                Holder<T> top;
                while (freePermits > 0 && (top = queue.tasks.peek()) != null) {
                    if (top.pendingPermits <= freePermits) {
                        top.execute();
                        freePermits -= top.pendingPermits;
                        queue.tasks.poll();
                        permitsPerTick.addAndGet((int) (currentTick % totalTicks), top.pendingPermits);
                    } else {
                        int permitsToConsume = ((int) (freePermits) + 1);
                        freePermits -= permitsToConsume;
                        top.pendingPermits -= permitsToConsume;
                        permitsPerTick.addAndGet((int) (currentTick % totalTicks), permitsToConsume);
                    }
                }

                if (freePermits <= 0) {
                    break;
                }
            }
        } finally {
            executionInProgress.set(false);
        }
    }

    private boolean moveWindow(long currentTick) {
        long newWindowBeginTick = currentTick - ticksInWindow;

        if (newWindowBeginTick == windowBeginTick) {
            return false;
        }
        for (long i = windowBeginTick; i < newWindowBeginTick; ++i) {
            int beginIdx = (int) (i % totalTicks);
            int endIdx = (int) ((i + ticksInWindow) % totalTicks);
            permitsConsumedInLastWindow += (permitsPerTick.get(endIdx) - permitsPerTick.getAndSet(beginIdx, 0));
        }

        windowBeginTick = newWindowBeginTick;
        return true;
    }

    private float getPermitsConsumedPrecise(long now, long currentTick) {
        float factor = ((float) (now % tickMs)) / tickMs;
        long beginningTick = currentTick - ticksInWindow;
        return permitsConsumedInLastWindow -
                (factor * permitsPerTick.get((int) (beginningTick % totalTicks))) +
                permitsPerTick.get((int) (currentTick % totalTicks));
    }

    private TaskQueue<T> getQueue(InternalQueueType type) {
        for (TaskQueue<T> queue : queues) {
            if (queue.type == type) {
                return queue;
            }
        }
        throw new IllegalStateException("Queue not found for type: " + type);
    }

    private ScheduledFuture<?> scheduleTask() {
        // schedule it at double the tick rate, so that we "don't miss" any tick changes.
        return scheduler.scheduleAtFixedRate(
                this::executePendingTasksInternal, 0, tickMs / 2,
                TimeUnit.MILLISECONDS
        );
    }

    @RequiredArgsConstructor
    static class TaskQueue<T> {
        private final InternalQueueType type;
        private final ConcurrentLinkedQueue<Holder<T>> tasks = new ConcurrentLinkedQueue<>();
    }

    static class Holder<T> {
        private final CompletableFuture<T> future;
        private final Supplier<CompletableFuture<T>> task;
        private int pendingPermits;

        public Holder(CompletableFuture<T> future, Supplier<CompletableFuture<T>> task, int pendingPermits) {
            this.future = future;
            this.task = task;
            this.pendingPermits = pendingPermits;
        }

        public void execute() {
            CompletableFuture<T> taskFuture = task.get();
            taskFuture.whenComplete((result, error) -> {
                if (error != null) {
                    future.completeExceptionally(error);
                } else {
                    future.complete(result);
                }
            });
        }
    }
}
