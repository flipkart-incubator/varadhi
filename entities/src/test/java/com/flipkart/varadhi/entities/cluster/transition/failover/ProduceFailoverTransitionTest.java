package com.flipkart.varadhi.entities.cluster.transition.failover;

import com.flipkart.varadhi.entities.cluster.transition.StageSnapshot;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.RegionName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.entities.cluster.transition.StageSnapshot.HostAck.ACKED;
import static com.flipkart.varadhi.entities.cluster.transition.StageSnapshot.HostAck.UNACKED;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProduceFailoverTransitionTest {

    @Test
    void historyRecordsOnlyFinishedStages() {
        ProduceFailoverTransition t = ProduceFailoverTransition.forFailover(
            "op-1",
            "proj.topic",
            RegionName.of("r1"),
            RegionName.of("r2"),
            false
        );
        t.advanceTo(TransitionStage.PREPARE, 7L, Map.of(), null);
        assertEquals(1, t.getStageHistory().size());
        assertEquals(TransitionStage.PENDING, t.getStageHistory().get(0).stage());
        assertEquals(TransitionStage.PREPARE, t.getCurrentStage());

        t.advanceTo(TransitionStage.FENCE, 7L, Map.of("host-a", ACKED, "host-b", UNACKED), null);

        assertEquals(2, t.getStageHistory().size());
        StageSnapshot prepare = t.getStageHistory().get(1);
        assertEquals(TransitionStage.PREPARE, prepare.stage());
        assertEquals(StageSnapshot.Outcome.OK, prepare.outcome());
        assertEquals(Map.of("host-a", ACKED, "host-b", UNACKED), prepare.hosts());
        assertEquals(TransitionStage.FENCE, t.getCurrentStage());

        t.advanceTo(TransitionStage.ABORTED, 7L, Map.of(), "barrier timeout");
        assertEquals(StageSnapshot.Outcome.FAILED, t.getStageHistory().get(2).outcome());
        assertEquals("barrier timeout", t.getStageHistory().get(2).errorMsg());
        assertEquals(TransitionStage.ABORTED, t.getCurrentStage());
    }

    @Test
    void recordedHostsStayStableWhenCallerMutatesTheMap() {
        ProduceFailoverTransition t = ProduceFailoverTransition.forFailover(
            "op-1",
            "proj.topic",
            RegionName.of("r1"),
            RegionName.of("r2"),
            false
        );
        Map<String, StageSnapshot.HostAck> hosts = new HashMap<>();
        hosts.put("host-a", ACKED);
        t.advanceTo(TransitionStage.PREPARE, 7L, hosts, null);
        hosts.put("host-a", UNACKED);
        hosts.put("host-b", ACKED);

        assertEquals(Map.of("host-a", ACKED), t.getStageHistory().get(0).hosts());
    }
}
