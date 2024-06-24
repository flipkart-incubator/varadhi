package com.flipkart.varadhi.consumer.concurrent;

import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;


/**
 * TODO: consider using vertx class
 *
 * Idea is:
 * Vertx : provides multithreaded runtime.
 * Event Executor : vertx's event loop
 * Subscription : Deployment
 *
 * Deployment -> choose Event loop (should be customizable). validate if possible in vertx.
 */
@RequiredArgsConstructor
public class EventExecutorGroup {

    final ScheduledExecutorService scheduler;
    private final ThreadFactory threadFactory;
    private final Supplier<BlockingQueue<Context.Task>> taskQueue;

    private final List<EventExecutor> executors = new CopyOnWriteArrayList<>();

    public EventExecutor newExecutor() {
        BlockingQueue<Context.Task> queue = taskQueue.get();
        EventExecutor executor = new EventExecutor(scheduler, threadFactory, queue);
        executors.add(executor);
        return executor;
    }
}
