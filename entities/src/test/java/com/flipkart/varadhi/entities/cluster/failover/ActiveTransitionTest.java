package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.RegionName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ActiveTransitionTest {

    @Test
    void historyRecordsOnlyFinishedStages() {
        ActiveTransition t = ActiveTransition.forFailover(
            "op-1",
            "proj.topic",
            RegionName.of("r1"),
            RegionName.of("r2"),
            false
        );
        t.advanceTo(TransitionStage.PREPARE, 7L, List.of(), null);
        assertEquals(1, t.getStageHistory().size());
        assertEquals(TransitionStage.PENDING, t.getStageHistory().get(0).stage());
        assertEquals(TransitionStage.PREPARE, t.getCurrentStage());

        t.advanceTo(TransitionStage.FENCE, 7L, List.of("host-a"), null);

        assertEquals(2, t.getStageHistory().size());
        StageSnapshot prepare = t.getStageHistory().get(1);
        assertEquals(TransitionStage.PREPARE, prepare.stage());
        assertEquals(StageSnapshot.Outcome.OK, prepare.outcome());
        assertEquals(List.of("host-a"), prepare.ackedHosts());
        assertEquals(TransitionStage.FENCE, t.getCurrentStage());

        t.advanceTo(TransitionStage.ABORTED, 7L, List.of(), "barrier timeout");
        assertEquals(StageSnapshot.Outcome.FAILED, t.getStageHistory().get(2).outcome());
        assertEquals("barrier timeout", t.getStageHistory().get(2).errorMsg());
        assertEquals(TransitionStage.ABORTED, t.getCurrentStage());
    }
}
