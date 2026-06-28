package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Controller-side per-stage ack barrier for topic failover. For each broadcast stage the executor
 * {@link #expect}s a barrier over the set of pods that must ack; pods reply with {@link TransitionAck}s
 * that {@link #recordAck} routes here by {@code opId}. The barrier completes when every expected host
 * has acked OK, and fails fast on the first failure ack, a timeout, or an explicit {@link #abort}.
 *
 * <p>Barriers confirm fleet-wide progress to the controller; they never gate produce routing (pods
 * route off the topic version), so a lost ack only delays controller knowledge — it cannot mis-route.
 */
@Slf4j
public class StageAwaiter {

    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<String, Barrier> barriers = new ConcurrentHashMap<>();

    public StageAwaiter() {
        this(
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("failover-awaiter-%d").setDaemon(true).build()
            )
        );
    }

    public StageAwaiter(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Registers a barrier for {@code (opId, stage)} over {@code expectedHosts} and arms a timeout.
     * An empty expected set completes immediately. Replaces any prior barrier for the same opId.
     */
    public CompletableFuture<Void> expect(
        String opId,
        TransitionStage stage,
        Set<String> expectedHosts,
        long timeoutMs
    ) {
        Barrier barrier = new Barrier(opId, stage, ConcurrentHashMap.newKeySet());
        barrier.expected.addAll(expectedHosts);
        barriers.put(opId, barrier);

        if (barrier.expected.isEmpty()) {
            log.info("Failover stage {} for op {} has no expected hosts; completing barrier immediately.", stage, opId);
            barrier.done.complete(null);
            return barrier.done;
        }
        barrier.timeout = scheduler.schedule(
            () -> barrier.fail(
                new FailoverAbortedException(
                    "stage " + stage + " timed out after " + timeoutMs + "ms; missing acks from " + barrier.missing()
                )
            ),
            timeoutMs,
            TimeUnit.MILLISECONDS
        );
        return barrier.done;
    }

    public void recordAck(TransitionAck ack) {
        Barrier barrier = barriers.get(ack.opId());
        if (barrier == null) {
            log.debug(
                "No active barrier for failover ack op={} host={} stage={}; ignoring.",
                ack.opId(),
                ack.hostname(),
                ack.stage()
            );
            return;
        }
        barrier.record(ack);
    }

    /** Fails the current barrier for {@code opId} (operator abort / pre-flight failure). */
    public void abort(String opId, String reason) {
        Barrier barrier = barriers.get(opId);
        if (barrier != null) {
            barrier.fail(new FailoverAbortedException(reason));
        }
    }

    /** Removes (and disarms) the barrier for {@code opId}. Call after each stage completes. */
    public void clear(String opId) {
        Barrier barrier = barriers.remove(opId);
        if (barrier != null) {
            barrier.cancelTimeout();
        }
    }

    private static final class Barrier {
        private final String opId;
        private final TransitionStage stage;
        private final Set<String> expected;
        private final Set<String> ackedOk = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<Void> done = new CompletableFuture<>();
        private volatile ScheduledFuture<?> timeout;

        private Barrier(String opId, TransitionStage stage, Set<String> expected) {
            this.opId = opId;
            this.stage = stage;
            this.expected = expected;
        }

        private synchronized void record(TransitionAck ack) {
            if (done.isDone()) {
                return;
            }
            if (ack.stage() != stage) {
                log.debug(
                    "Stale failover ack for op={} host={}: expected stage {} got {}; ignoring.",
                    opId,
                    ack.hostname(),
                    stage,
                    ack.stage()
                );
                return;
            }
            if (!ack.success()) {
                fail(
                    new FailoverAbortedException(
                        "stage " + stage + " failed on host " + ack.hostname() + ": " + ack.errorMsg()
                    )
                );
                return;
            }
            ackedOk.add(ack.hostname());
            if (ackedOk.containsAll(expected)) {
                cancelTimeout();
                done.complete(null);
            }
        }

        private synchronized void fail(Throwable t) {
            if (done.isDone()) {
                return;
            }
            cancelTimeout();
            done.completeExceptionally(t);
        }

        private Set<String> missing() {
            Set<String> missing = ConcurrentHashMap.newKeySet();
            missing.addAll(expected);
            missing.removeAll(ackedOk);
            return missing;
        }

        private void cancelTimeout() {
            ScheduledFuture<?> t = timeout;
            if (t != null) {
                t.cancel(false);
            }
        }
    }
}
