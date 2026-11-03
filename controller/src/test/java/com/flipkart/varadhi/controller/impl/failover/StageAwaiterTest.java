package com.flipkart.varadhi.controller.impl.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.transition.TransitionAck;
import com.flipkart.varadhi.entities.cluster.transition.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageAwaiterTest {

    @Test
    void successfulAckCompletesStageBarrier() {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            StageAwaiter awaiter = new StageAwaiter(scheduler);
            var barrier = awaiter.expect("op-1", TransitionStage.PREPARE, Set.of("host-1"), 60_000);

            awaiter.recordAck(
                TransitionAck.success(
                    "op-1",
                    VaradhiTopicName.parse("project.topic"),
                    TransitionType.TOPIC_FAILOVER,
                    TransitionParticipation.NOT_INVOLVED,
                    "host-1",
                    TransitionStage.PREPARE
                )
            );

            assertTrue(barrier.isDone());
            assertDoesNotThrow(barrier::join);
            awaiter.clear("op-1");
        } finally {
            scheduler.shutdownNow();
        }
    }
}
