package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.concurrent.CustomThread;
import com.flipkart.varadhi.consumer.concurrent.EventExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyControlImplTest {

    private static final InternalQueueType mainQ = new InternalQueueType.Main();
    private static final InternalQueueType[] priority = new InternalQueueType[] {new InternalQueueType.Retry(3),
        new InternalQueueType.Retry(2), new InternalQueueType.Retry(1), mainQ};


    private static final EventExecutor executor = new EventExecutor(
        null,
        CustomThread::new,
        new LinkedBlockingQueue<>()
    );

    @AfterAll
    public static void shutdown() {
        executor.stop();
    }

    @Test
    @Timeout (5)
    public void testPriorityOrderingAndConcurrencyIsFollowed() {
        Context ctx = new Context(executor);
        ctx.updateCurrentThreadContext();

        ConcurrencyControlImpl<Integer> cc = new ConcurrencyControlImpl<>(ctx, 2, priority);

        // tasks that have been executed by the CC
        List<TaskWithType> executed = Collections.synchronizedList(new LinkedList<>());

        // tasks that have been enqueued to the CC
        Map<InternalQueueType, List<CompletableFuture<Integer>>> enqueued = new HashMap<>();
        Arrays.stream(priority).forEach(t -> enqueued.put(t, new ArrayList<>()));
        Supplier<Integer> enqueuedSize = () -> enqueued.values().stream().mapToInt(List::size).sum();

        List<InternalQueueType> types = new ArrayList<>(Arrays.stream(priority).toList());
        Collections.reverse(types);

        types.forEach(t -> {
            // create 3 tasks
            List<Supplier<CompletableFuture<Integer>>> tasks = Arrays.asList(
                taskSupplier(t, executed),
                taskSupplier(t, executed),
                taskSupplier(t, executed)
            );

            // we need to enqueue tasks from the context thread, as that is the requirement of the CC.
            CompletableFuture<?> enqueueFuture = new CompletableFuture<>();
            executor.execute(new EventExecutor.WrappedTask(ctx, () -> {
                enqueued.get(t).addAll(cc.enqueueTasks(t, tasks));
                enqueueFuture.complete(null);
            }));
            enqueueFuture.join();
        });

        // The concurrency is set to 2, but we have enqueued 3 tasks for each priority in reverse priority order.
        assertEquals(2, executed.size());
        assertEquals(12, enqueuedSize.get());
        assertEquals(10, cc.getPendingCount());

        // all executed tasks should be from MQ.
        assertTrue(executed.stream().allMatch(t -> t.type == mainQ));

        // lets complete the 2 executed tasks, then next 2 tasks should be from rq3.
        IntStream.range(0, 2).forEach(_i -> {
            executed.remove(0).task.complete(0);
            enqueued.get(mainQ).remove(0).join();
        });
        // wait for executed.size() to be 2 again
        await().untilAsserted(() -> assertEquals(2, executed.size()));
        assertEquals(8, cc.getPendingCount());
        assertTrue(executed.stream().allMatch(t -> t.type == priority[0]));

        // execute the rest. The fact that below succeeds, means that we are able to follow ordering, priority & concurrency.
        long start = System.currentTimeMillis();
        int pendingCompletion = 10;
        for (InternalQueueType t : priority) {
            for (CompletableFuture<Integer> f : enqueued.get(t)) {
                executed.remove(0).task.complete(0);
                f.join();
                pendingCompletion--;
                int _p = pendingCompletion;
                await().pollInterval(Duration.ofMillis(1))
                       .untilAsserted(() -> assertEquals(Math.min(2, _p), executed.size()));
            }
        }
    }

    private static Supplier<CompletableFuture<Integer>> taskSupplier(InternalQueueType t, List<TaskWithType> executed) {
        return () -> {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            executed.add(new TaskWithType(t, task));
            return task;
        };
    }

    record TaskWithType(InternalQueueType type, CompletableFuture<Integer> task) {
    }
}
