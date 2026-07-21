package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionAck;
import com.flipkart.varadhi.entities.cluster.failover.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageAwaiterTest {

    private static final String OP = "op-1";
    private static final VaradhiTopicName TOPIC = VaradhiTopicName.of("proj", "topic1");

    @Test
    void completesWhenAllExpectedHostsAckOk() throws Exception {
        StageAwaiter awaiter = new StageAwaiter();
        CompletableFuture<Void> barrier = awaiter.expect(OP, TransitionStage.PREPARE, Set.of("h1", "h2"), 5_000);

        awaiter.recordAck(successAck("h1", TransitionStage.PREPARE));
        assertFalse(barrier.isDone());
        awaiter.recordAck(successAck("h2", TransitionStage.PREPARE));

        barrier.get(2, TimeUnit.SECONDS);
        assertTrue(barrier.isDone());
        assertFalse(barrier.isCompletedExceptionally());
    }

    @Test
    void completesImmediatelyWhenNoExpectedHosts() throws Exception {
        StageAwaiter awaiter = new StageAwaiter();
        CompletableFuture<Void> barrier = awaiter.expect(OP, TransitionStage.SWITCH, Set.of(), 5_000);
        barrier.get(1, TimeUnit.SECONDS);
        assertTrue(barrier.isDone());
    }

    @Test
    void failsFastOnFailureAck() {
        StageAwaiter awaiter = new StageAwaiter();
        CompletableFuture<Void> barrier = awaiter.expect(OP, TransitionStage.PREPARE, Set.of("h1", "h2"), 5_000);

        awaiter.recordAck(
            TransitionAck.failure(
                OP,
                TOPIC,
                TransitionType.TOPIC_FAILOVER,
                TransitionParticipation.INVOLVED,
                "h1",
                TransitionStage.PREPARE,
                "boom"
            )
        );

        ExecutionException ex = assertThrows(ExecutionException.class, () -> barrier.get(2, TimeUnit.SECONDS));
        assertInstanceOf(FailoverAbortedException.class, ex.getCause());
    }

    @Test
    void failsOnTimeout() {
        StageAwaiter awaiter = new StageAwaiter();
        CompletableFuture<Void> barrier = awaiter.expect(OP, TransitionStage.SWITCH, Set.of("h1"), 100);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> barrier.get(2, TimeUnit.SECONDS));
        assertInstanceOf(FailoverAbortedException.class, ex.getCause());
    }

    @Test
    void abortFailsBarrier() {
        StageAwaiter awaiter = new StageAwaiter();
        CompletableFuture<Void> barrier = awaiter.expect(OP, TransitionStage.PREPARE, Set.of("h1"), 5_000);

        awaiter.abort(OP, "operator abort");

        ExecutionException ex = assertThrows(ExecutionException.class, () -> barrier.get(2, TimeUnit.SECONDS));
        assertInstanceOf(FailoverAbortedException.class, ex.getCause());
    }

    @Test
    void ignoresAckForDifferentStage() {
        StageAwaiter awaiter = new StageAwaiter();
        CompletableFuture<Void> barrier = awaiter.expect(OP, TransitionStage.SWITCH, Set.of("h1"), 5_000);

        // A stale PREPARE ack must not satisfy a SWITCH barrier.
        awaiter.recordAck(successAck("h1", TransitionStage.PREPARE));

        assertThrows(TimeoutException.class, () -> barrier.get(300, TimeUnit.MILLISECONDS));
        assertFalse(barrier.isDone());
    }

    @Test
    void clearRemovesBarrierSoLateAcksAreIgnored() throws Exception {
        StageAwaiter awaiter = new StageAwaiter();
        CompletableFuture<Void> barrier = awaiter.expect(OP, TransitionStage.PREPARE, Set.of("h1"), 5_000);
        awaiter.recordAck(successAck("h1", TransitionStage.PREPARE));
        barrier.get(1, TimeUnit.SECONDS);

        awaiter.clear(OP);
        // No active barrier now; a late ack is simply ignored (no exception).
        awaiter.recordAck(successAck("h1", TransitionStage.PREPARE));
    }

    private static TransitionAck successAck(String hostname, TransitionStage stage) {
        return TransitionAck.success(
            OP,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.INVOLVED,
            hostname,
            stage
        );
    }
}
