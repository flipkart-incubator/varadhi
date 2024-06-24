package com.flipkart.varadhi.consumer.concurrent;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class EventExecutor implements Executor {

    private final ScheduledExecutorService scheduler;

    private final CustomThread thread;

    private final BlockingQueue<Context.Task> taskQueue;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public EventExecutor(
            ScheduledExecutorService scheduler, ThreadFactory threadFactory, BlockingQueue<Context.Task> taskQueue
    ) {
        this.scheduler = scheduler;
        this.taskQueue = taskQueue;
        this.thread = (CustomThread) threadFactory.newThread(this::run);
        this.thread.start();
    }

    CustomThread getThread() {
        return thread;
    }

    @Override
    public void execute(Runnable command) {
        // TODO: review all cases here. As of now, here we are assuming that all tasks are running from custom threads,
        //  which is most likely false.

        if (command instanceof Context.Task task) {

            // TODO: maybe move this check to context.execute()
            EventExecutor boundExecutor = task.getContext().getExecutor();
            if (boundExecutor != null && boundExecutor != this) {
                throw new IllegalStateException(
                        "task is tied to an executor:" + boundExecutor + ", but is being executed on:" + this);
            }
            taskQueue.add(task);
            return;
        }

        taskQueue.add(new WrappedTask(Context.getCurrentThreadContext(), command));
    }

    public void stop() {
        running.set(false);
    }

    public ScheduledFuture<?> schedule(Context.Task command, long delay, TimeUnit unit) {
        return scheduler.schedule(() -> execute(command), delay, unit);
    }

    void run() {
        try {
            while (running.getPlain()) {
                Context.Task task = taskQueue.take();
                // TODO: right now we are setting it to the thread's field. But we may want to unify the context storage by using FastThreadLocal.
                thread.setContext(task.getContext());
                runSafely(task);

                // probably can be removed
                thread.setContext(null);
            }
        } catch (InterruptedException e) {
            log.warn("EventExecutor interrupted. Exiting", e);
            Thread.currentThread().interrupt();
        }
    }

    void runSafely(Context.Task task) {
        try {
            task.run();
        } catch (Throwable t) {
            log.error("Error while executing task: {}", task, t);
        }
    }

    @AllArgsConstructor
    public static class WrappedTask implements Context.Task {
        private final Context ctx;
        private final Runnable runnable;

        @Override
        public Context getContext() {
            return ctx;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }
}
