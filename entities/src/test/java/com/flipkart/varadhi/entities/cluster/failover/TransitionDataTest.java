package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.cluster.Operation;
import com.flipkart.varadhi.entities.cluster.TopicFailoverOperation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionDataTest {

    @Test
    void opStartsWithoutTransitionData_andRetryKeepsItAbsent() {
        TopicFailoverOperation op = failoverOp();
        assertNull(op.getTransitionData());
        assertFalse(JsonMapper.jsonSerialize(op).contains("transitionData"));

        TopicFailoverOperation restored = JsonMapper.jsonDeserialize(
            JsonMapper.jsonSerialize(op),
            TopicFailoverOperation.class
        );
        assertNull(restored.getTransitionData());

        TopicFailoverOperation retry = op.nextRetry();
        assertNull(retry.getTransitionData());
        assertEquals(1, retry.getRetryAttempt());
    }

    @Test
    void attachOnComplete_snapshotsMasterAndSurvivesReload() {
        TransitionMaster master = TransitionMaster.forFailover(
            "op-1",
            "proj.topic",
            RegionName.of("r1"),
            RegionName.of("r2"),
            true,
            "user"
        );
        master.beginStage(TransitionStage.PREPARE);
        master.advanceTo(TransitionStage.PREPARE, 7L);
        master.closeOpenStage(StageSnapshot.Outcome.OK, null);
        master.advanceTo(TransitionStage.COMPLETED, 7L);

        TopicFailoverOperation op = failoverOp();
        op.attachTransitionData(TransitionData.from(master));
        op.markCompleted();
        assertNull(op.nextRetry().getTransitionData());

        master.beginStage(TransitionStage.ABORTED);
        master.getStageHistory().get(0).setOutcome(StageSnapshot.Outcome.FAILED);

        TransitionData data = op.getTransitionData();
        assertEquals(TransitionStage.COMPLETED, data.getCurrentStage());
        assertTrue(data.isWaitForReplicationLagToClear());
        assertEquals("user", data.getRequestedBy());
        assertEquals(1, data.getStageHistory().size());
        assertEquals(TransitionStage.PREPARE, data.getStageHistory().get(0).getStage());
        assertEquals(StageSnapshot.Outcome.OK, data.getStageHistory().get(0).getOutcome());

        TopicFailoverOperation restored = JsonMapper.jsonDeserialize(
            JsonMapper.jsonSerialize(op),
            TopicFailoverOperation.class
        );
        assertEquals(Operation.State.COMPLETED, restored.getState());
        assertEquals(7L, restored.getTransitionData().getTopicVersionToAwait());
        assertEquals(StageSnapshot.Outcome.OK, restored.getTransitionData().getStageHistory().get(0).getOutcome());

        TopicFailoverOperation stored = failoverOp();
        stored.applyProgressFrom(op);
        assertEquals(Operation.State.COMPLETED, stored.getState());
        assertEquals(1, stored.getTransitionData().getStageHistory().size());
    }

    @Test
    void attachRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> failoverOp().attachTransitionData(null));
    }

    private static TopicFailoverOperation failoverOp() {
        return TopicFailoverOperation.of("proj.topic");
    }
}
