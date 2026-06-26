package com.flipkart.varadhi.entities.cluster.failover;

import com.flipkart.varadhi.entities.VaradhiTopicName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionEventTest {

    private static final String OP_ID = "op-1";
    private static final VaradhiTopicName TOPIC = VaradhiTopicName.of("proj", "topic");

    @Test
    void of_prepare_setsVersionGatedFields() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.PREPARE,
            10L,
            "region-b"
        );

        assertEquals(TransitionStage.PREPARE, event.stage());
        assertTrue(event.awaitVersion());
        assertEquals(10L, event.topicVersionToAwait());
        assertEquals("region-b", event.target());
    }

    @Test
    void of_switch_clearsTarget() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.SWITCH,
            11L,
            "ignored"
        );

        assertEquals(TransitionStage.SWITCH, event.stage());
        assertTrue(event.awaitVersion());
        assertEquals(11L, event.topicVersionToAwait());
        assertNull(event.target());
    }

    @Test
    void of_immediateAckStage_ignoresVersionAndTarget() {
        TransitionEvent event = TransitionEvent.of(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.COMPLETED,
            99L,
            "ignored"
        );

        assertFalse(event.awaitVersion());
        assertEquals(0L, event.topicVersionToAwait());
        assertNull(event.target());
    }

    @Test
    void of_prepare_requiresTarget() {
        assertThrows(
            IllegalArgumentException.class,
            () -> TransitionEvent.of(OP_ID, TOPIC, TransitionType.TOPIC_FAILOVER, TransitionStage.PREPARE, 10L, null)
        );
    }

    @Test
    void constructor_rejectsMismatchedAwaitVersion() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new TransitionEvent(
                OP_ID,
                TOPIC,
                TransitionType.TOPIC_FAILOVER,
                TransitionStage.SWITCH,
                false,
                11L,
                null
            )
        );
    }
}
