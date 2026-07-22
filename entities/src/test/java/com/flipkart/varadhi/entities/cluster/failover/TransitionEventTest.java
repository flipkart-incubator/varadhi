package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionEventTest {

    private static final String OP_ID = "op-1";
    private static final VaradhiTopicName TOPIC = VaradhiTopicName.of("proj", "topic");

    @Test
    void of_preservesControllerDrivenWireFields() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.PREPARE,
            true,
            10L,
            "region-b"
        );

        assertEquals(TransitionStage.PREPARE, event.stage());
        assertTrue(event.awaitVersion());
        assertEquals(10L, event.topicVersionToAwait());
        assertEquals("region-b", event.target());
    }

    @Test
    void of_switchPreservesTargetWhenControllerSetsIt() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.SWITCH,
            true,
            11L,
            "ignored-by-handler"
        );

        assertEquals(TransitionStage.SWITCH, event.stage());
        assertTrue(event.awaitVersion());
        assertEquals(11L, event.topicVersionToAwait());
        assertEquals("ignored-by-handler", event.target());
    }

    @Test
    void of_immediateAckStageUsesControllerAwaitVersionFalse() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.COMPLETED,
            false,
            99L,
            "ignored"
        );

        assertFalse(event.awaitVersion());
        assertEquals(99L, event.topicVersionToAwait());
        assertEquals("ignored", event.target());
    }

    @Test
    void of_prepare_allowsVersionZero() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.PREPARE,
            true,
            0L,
            "region-b"
        );

        assertEquals(0L, event.topicVersionToAwait());
    }

    @Test
    void of_prepare_allowsNullTarget() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.PREPARE,
            true,
            10L,
            null
        );

        assertNull(event.target());
    }
}
