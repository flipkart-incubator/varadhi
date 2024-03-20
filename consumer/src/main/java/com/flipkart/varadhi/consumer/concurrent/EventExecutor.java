package com.flipkart.varadhi.consumer.concurrent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

@Slf4j
@RequiredArgsConstructor
public class EventExecutor implements Executor {

    private final EventExecutorGroup parent;

    private final CustomThread thread;

    private final BlockingQueue<Runnable> taskQueue;

    public EventExecutor(EventExecutorGroup group, ThreadFactory threadFactory, BlockingQueue<Runnable> taskQueue) {
        this.parent = group;
        this.thread = (CustomThread) threadFactory.newThread(this::run);
        this.taskQueue = taskQueue;
    }

    EventExecutorGroup getParent() {
        return parent;
    }


    @Override
    public void execute(Runnable command) {
        // TODO: fix the implementation
        taskQueue.offer(command);
    }

    void run() {
        try {
            while (true) {
                Runnable task = taskQueue.take();
                runSafely(task);
            }
        } catch (InterruptedException e) {
            log.warn("EventExecutor interrupted. Exiting", e);
            Thread.currentThread().interrupt();
        }
    }

    void runSafely(Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            log.error("Error while executing task: {}", task, t);
        }
    }
}
