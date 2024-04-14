package com.flipkart.varadhi.consumer.concurrent;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class EventExecutor implements Executor {

    private final EventExecutorGroup parent;

    private final CustomThread thread;

    private final BlockingQueue<Context.Task> taskQueue;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public EventExecutor(EventExecutorGroup group, ThreadFactory threadFactory, BlockingQueue<Context.Task> taskQueue) {
        this.parent = group;
        this.taskQueue = taskQueue;
        this.thread = (CustomThread) threadFactory.newThread(this::run);
        this.thread.start();
    }

    EventExecutorGroup getParent() {
        return parent;
    }

    @Override
    public void execute(Runnable command) {
        // TODO: review all cases here. As of now, here we are assuming that all tasks are running from custom threads,
        //  which is most likely false.

        if (command instanceof Context.Task) {
            EventExecutor boundExecutor = ((Context.Task) command).getContext().getExecutor();
            if (boundExecutor != null && boundExecutor != this) {
                throw new IllegalStateException(
                        "task is tied to an executor:" + boundExecutor + ", but is being executed on:" + this);
            }
            taskQueue.add((Context.Task) command);
            return;
        }

        taskQueue.add(new WrappedTask(Context.getCurrentTheadContext(), command));
    }

    public void stop() {
        //TODO: is plain set/get fine?
        running.setPlain(false);
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
