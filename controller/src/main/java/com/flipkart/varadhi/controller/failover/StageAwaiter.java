package com.flipkart.varadhi.controller.failover;

import com.flipkart.varadhi.entities.cluster.failover.FailoverStage;
import com.flipkart.varadhi.entities.cluster.failover.PodAckSnapshot;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Controller-side per-(opId, stage) barrier.
 *
 * <p>Created when the executor calls
 * {@code expect(stage, hosts, timeoutMs, resender)} just before broadcasting a
 * {@link com.flipkart.varadhi.entities.cluster.failover.FailoverStageEvent}.  Pods reply via
 * {@code MessageExchange.send} carrying a {@link com.flipkart.varadhi.entities.cluster.failover.FailoverStatusUpdate};
 * those updates are funneled to {@link #recordAck(String, boolean, String)}.
 *
 * <p>Completion semantics:
 * <ul>
 *   <li>{@link #future} completes normally when every host in {@code expected}
 *       has acked OK at least once.</li>
 *   <li>It completes exceptionally on timeout (after an optional pre-timeout
 *       targeted resend) or when any host reports a hard failure.</li>
 * </ul>
 *
 * <p>{@link #addExpected(String)} / {@link #markHostGone(String)} hooks let the
 * controller adjust the expected set as cluster membership changes, so a pod that
 * joins mid-stage is still required to ack and a pod that dies cleanly mid-stage
 * does not stall the barrier.
 */
@Slf4j
public final class StageAwaiter {

    private final String opId;
    private final FailoverStage stage;
    private final long fenceVersion;
    private final long timeoutMs;
    private final Consumer<Set<String>> resender;
    private final ScheduledThreadPoolExecutor scheduler;

    private final Set<String> expected = new HashSet<>();
    private final Set<String> ackedOk = new HashSet<>();
    private final List<PodAckSnapshot> finalAcks = new ArrayList<>();
    private final CompletableFuture<List<PodAckSnapshot>> future = new CompletableFuture<>();
    private final Object lock = new Object();

    private volatile boolean closed;
    private ScheduledFuture<?> timeoutTask;
    private ScheduledFuture<?> resendTask;

    StageAwaiter(
        String opId,
        FailoverStage stage,
        long fenceVersion,
        Set<String> initialExpected,
        long timeoutMs,
        long resendAfterMs,
        Consumer<Set<String>> resender,
        ScheduledThreadPoolExecutor scheduler
    ) {
        this.opId = opId;
        this.stage = stage;
        this.fenceVersion = fenceVersion;
        this.timeoutMs = timeoutMs;
        this.resender = resender;
        this.scheduler = scheduler;
        this.expected.addAll(initialExpected);

        this.timeoutTask = scheduler.schedule(this::onTimeout, timeoutMs, TimeUnit.MILLISECONDS);
        if (resender != null && resendAfterMs > 0 && resendAfterMs < timeoutMs) {
            this.resendTask = scheduler.schedule(this::onResendTick, resendAfterMs, TimeUnit.MILLISECONDS);
        }
    }

    public String opId() {
        return opId;
    }

    public FailoverStage stage() {
        return stage;
    }

    public long fenceVersion() {
        return fenceVersion;
    }

    public CompletableFuture<List<PodAckSnapshot>> future() {
        return future;
    }

    /**
     * Records an ack from {@code host}. If {@code ok==false} the barrier completes
     * exceptionally — the executor is expected to fail and (optionally) retry the op.
     */
    public void recordAck(String host, boolean ok, String errorMsg) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (!expected.contains(host)) {
                log.debug("Discarding unexpected ack for op={} stage={} host={}", opId, stage, host);
                return;
            }
            finalAcks.add(new PodAckSnapshot(host, ok, System.currentTimeMillis(), errorMsg));
            if (!ok) {
                closeWithError(new RuntimeException(
                    "Pod " + host + " failed stage " + stage + " of op " + opId + ": " + errorMsg
                ));
                return;
            }
            ackedOk.add(host);
            if (ackedOk.containsAll(expected)) {
                closeOk();
            }
        }
    }

    /** New pod joined mid-stage; require an ack from it too. */
    public void addExpected(String host) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (expected.add(host)) {
                log.info("Op {} stage {}: added late-joiner {} to expected set", opId, stage, host);
                if (resender != null) {
                    resender.accept(Set.of(host));
                }
            }
        }
    }

    /** Host left the cluster; drop it from the expected set. */
    public void markHostGone(String host) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (expected.remove(host)) {
                ackedOk.remove(host);
                log.info("Op {} stage {}: host {} left; dropped from expected set", opId, stage, host);
                if (ackedOk.containsAll(expected)) {
                    closeOk();
                }
            }
        }
    }

    private void onResendTick() {
        synchronized (lock) {
            if (closed || resender == null) {
                return;
            }
            Set<String> missing = new HashSet<>(expected);
            missing.removeAll(ackedOk);
            if (!missing.isEmpty()) {
                log.info("Op {} stage {}: resending to {} missing hosts", opId, stage, missing.size());
                resender.accept(missing);
            }
        }
    }

    private void onTimeout() {
        Set<String> missing;
        synchronized (lock) {
            if (closed) {
                return;
            }
            missing = new HashSet<>(expected);
            missing.removeAll(ackedOk);
        }
        closeWithError(new RuntimeException(
            "Stage " + stage + " of op " + opId + " timed out after " + timeoutMs + "ms; missing acks from: " + missing
        ));
    }

    private void closeOk() {
        closed = true;
        cancelTasks();
        future.complete(new ArrayList<>(finalAcks));
    }

    private void closeWithError(Throwable t) {
        closed = true;
        cancelTasks();
        future.completeExceptionally(t);
    }

    private void cancelTasks() {
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
        if (resendTask != null) {
            resendTask.cancel(false);
        }
    }

    /** Returns the current expected set (snapshot). For tests / inspection only. */
    public Set<String> expectedSnapshot() {
        synchronized (lock) {
            return Set.copyOf(expected);
        }
    }
}
