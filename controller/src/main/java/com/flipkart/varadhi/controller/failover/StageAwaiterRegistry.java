package com.flipkart.varadhi.controller.failover;

import com.flipkart.varadhi.entities.cluster.failover.FailoverStage;
import com.flipkart.varadhi.entities.cluster.failover.PodAckSnapshot;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * Controller-wide store of live {@link StageAwaiter}s keyed by {@code (opId, stage)}.
 *
 * <p>Used by:
 * <ul>
 *   <li>{@code TopicFailoverOpExecutor} to create / await barriers per stage.</li>
 *   <li>{@code OperationMgr.recordFailoverAck} to route incoming
 *       {@link com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate} messages.</li>
 *   <li>{@code MembershipListener} to call {@link StageAwaiter#markHostGone(String)} /
 *       {@link StageAwaiter#addExpected(String)} as the cluster changes.</li>
 * </ul>
 */
@Slf4j
public final class StageAwaiterRegistry implements AutoCloseable {

    private final Map<Key, StageAwaiter> awaiters = new ConcurrentHashMap<>();
    private final ScheduledThreadPoolExecutor scheduler;

    public StageAwaiterRegistry(int schedulerThreads) {
        this.scheduler = new ScheduledThreadPoolExecutor(
            Math.max(1, schedulerThreads),
            new ThreadFactoryBuilder().setNameFormat("stage-awaiter-%d").setDaemon(true).build()
        );
        this.scheduler.setRemoveOnCancelPolicy(true);
    }

    /**
     * Create a barrier for {@code (opId, stage)} and return its completion future.
     * The awaiter takes ownership of {@code expected} (it is copied internally) and
     * fires {@code resender(Set<String>)} after {@code resendAfterMs} for hosts that
     * have not yet acked. After {@code timeoutMs} it completes exceptionally.
     *
     * <p>Caller must {@link #remove(String, FailoverStage)} the entry once the
     * returned future has been observed.
     */
    public CompletableFuture<List<PodAckSnapshot>> expect(
        String opId,
        FailoverStage stage,
        long fenceVersion,
        Set<String> expected,
        long timeoutMs,
        long resendAfterMs,
        Consumer<Set<String>> resender
    ) {
        StageAwaiter awaiter = new StageAwaiter(
            opId,
            stage,
            fenceVersion,
            expected,
            timeoutMs,
            resendAfterMs,
            resender,
            scheduler
        );
        StageAwaiter prev = awaiters.put(new Key(opId, stage), awaiter);
        if (prev != null) {
            log.warn("Replacing existing StageAwaiter for opId={} stage={}", opId, stage);
            prev.future().completeExceptionally(new IllegalStateException("Replaced by new awaiter"));
        }
        return awaiter.future();
    }

    public StageAwaiter get(String opId, FailoverStage stage) {
        return awaiters.get(new Key(opId, stage));
    }

    public StageAwaiter remove(String opId, FailoverStage stage) {
        return awaiters.remove(new Key(opId, stage));
    }

    /** All currently live awaiters across all in-flight ops. */
    public java.util.Collection<StageAwaiter> all() {
        return awaiters.values();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    private record Key(String opId, FailoverStage stage) {}
}
