package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.MockTicker;
import com.flipkart.varadhi.consumer.InternalQueueType;
import com.google.common.base.Ticker;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;

@Slf4j
class SlidingWindowThrottlerTest {

    private final InternalQueueType[] priority = new InternalQueueType[]{
            new InternalQueueType.Retry(3),
            new InternalQueueType.Retry(2),
            new InternalQueueType.Retry(1),
            new InternalQueueType.Main()
    };
    private final InternalQueueType mainQ = priority[3];

    private final ScheduledExecutorService noopScheduler = mock(ScheduledExecutorService.class);
    private final ScheduledExecutorService defaultScheduler = Executors.newSingleThreadScheduledExecutor();

    // TODO: add more tests. Ordered execution of tasks.

    @Test
    public void testExecutePendingTasksFollowsRateLimit() throws Exception {
        // round off last digits, so that we can simulate time advancement exactly
        var ticker = new MockTicker(System.currentTimeMillis() / 10 * 10_000_000);
        int qps = 10;
        int expectedCompleted = 0;

        try (var throttler = new SlidingWindowThrottler<Integer>(noopScheduler, ticker, qps, 1000, 10, priority)) {

            // acquire 1000 permits
            List<CompletableFuture<Integer>> tasks = new ArrayList<>();
            Set<Integer> completed = new ConcurrentHashSet<>();
            for (int i = 0; i < 1000; i++) {
                int id = i;
                tasks.add(throttler.acquire(mainQ, () -> CompletableFuture.completedFuture(id), 1)
                        .whenComplete((r, e) -> {
                            completed.add(r);
                        }));
            }

            Consumer<Integer> assertions = (completedCount) -> {
                throttler.executePendingTasksInternal();
                Assertions.assertEquals(completedCount, completed.size());
                Assertions.assertEquals(
                        IntStream.range(0, completedCount).boxed().collect(Collectors.toSet()),
                        new HashSet<>(completed)
                );
            };

            // validate none of the tasks are finished
            tasks.forEach(t -> Assertions.assertFalse(t.isDone()));

            // try to launch enqueued tasks. since current rate is 0, it should launch exactly qps tasks
            expectedCompleted += qps;
            assertions.accept(expectedCompleted);

            ticker.advance(900, TimeUnit.MILLISECONDS);
            assertions.accept(expectedCompleted);
            // nwo we are at time 1 sec & 5 ms ahead of the start time. since tick rate is 10 ms, and we are in middle of
            // the tick, we should be able to launch 5 more tasks.
            ticker.advance(105, TimeUnit.MILLISECONDS);
            expectedCompleted += (qps / 2);
            assertions.accept(expectedCompleted);

            // there is a gotcha here. advancing 5 more ms, will lead to tick change. so new tasks's permits will get
            // assigned to new tick bucket.
            ticker.advance(5, TimeUnit.MILLISECONDS);
            expectedCompleted += (qps / 2);
            assertions.accept(expectedCompleted);

            // now after 1 sec exact, the previous 5 tasks's permits will start getting released.
            // so if we wait another 10 ms, all permits should be freed up.
            ticker.advance(1010, TimeUnit.MILLISECONDS);
            expectedCompleted += qps;
            assertions.accept(expectedCompleted);
        }
    }

    @Test
    public void testRateLimitBehaviourAndPriorityOverMultipleWindow() throws Exception {
        try (
                var throttler = new SlidingWindowThrottler<Integer>(
                        defaultScheduler, Ticker.systemTicker(), 10, 1000, 10, priority)
        ) {
            long start = System.currentTimeMillis();
            CountDownLatch latch = new CountDownLatch(21);
            List<Integer> taskTypeCompleted = new ArrayList<>();
            for (int i = 0; i < 21; ++i) {
                int _i = i;
                throttler.acquire(priority[i % 4], () -> CompletableFuture.completedFuture(_i % 4), 1)
                        .whenComplete((r, e) -> {
                            latch.countDown();
                            taskTypeCompleted.add(r);
                        });
            }
            Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
            long duration = System.currentTimeMillis() - start;

            Assertions.assertTrue(duration >= 1990 && duration <= 3000, "duration is: " + duration);
            Assertions.assertEquals(taskTypeCompleted.stream().sorted().toList(), taskTypeCompleted);
        }
    }
}
