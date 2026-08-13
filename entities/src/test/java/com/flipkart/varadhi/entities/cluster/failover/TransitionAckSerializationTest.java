package com.flipkart.varadhi.entities.cluster.failover;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionAckSerializationTest {

    private static final String OP_ID = "op-1";
    private static final VaradhiTopicName TOPIC = VaradhiTopicName.of("proj", "topic");
    private static final String HOST = "host-1";

    @Test
    void serialize_omitsDerivedSuccessAndFailureKeys() throws Exception {
        TransitionAck ok = TransitionAck.success(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.INVOLVED,
            HOST,
            TransitionStage.PREPARE
        );
        TransitionAck fail = TransitionAck.failure(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.INVOLVED,
            HOST,
            TransitionStage.PREPARE,
            "warm failed"
        );

        assertDerivedKeysAbsent(JsonMapper.jsonSerialize(ok));
        assertDerivedKeysAbsent(JsonMapper.jsonSerialize(fail));
    }

    @Test
    void roundTrip_preservesWireFieldsAndDerivedOutcome() {
        TransitionAck original = TransitionAck.failure(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.NOT_INVOLVED,
            HOST,
            TransitionStage.FENCE,
            "timeout"
        );

        TransitionAck restored = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(original), TransitionAck.class);

        assertEquals(original.opId(), restored.opId());
        assertEquals(original.topicFqn(), restored.topicFqn());
        assertEquals(original.transitionType(), restored.transitionType());
        assertEquals(original.participation(), restored.participation());
        assertEquals(original.hostname(), restored.hostname());
        assertEquals(original.stage(), restored.stage());
        assertEquals(original.errorMsg(), restored.errorMsg());
        assertTrue(restored.isFailure());
        assertFalse(restored.isSuccess());
    }

    @Test
    void roundTrip_successHasNullErrorMsg() {
        TransitionAck original = TransitionAck.success(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.INVOLVED,
            HOST,
            TransitionStage.COMPLETED
        );

        TransitionAck restored = JsonMapper.jsonDeserialize(JsonMapper.jsonSerialize(original), TransitionAck.class);

        assertNull(restored.errorMsg());
        assertTrue(restored.isSuccess());
        assertFalse(restored.isFailure());
    }

    @Test
    void isSuccess_whitespaceOnlyErrorMsgIsSuccess() {
        TransitionAck ack = new TransitionAck(
            OP_ID,
            TOPIC,
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.INVOLVED,
            HOST,
            TransitionStage.PREPARE,
            "   ",
            null
        );
        assertTrue(ack.isSuccess());
        assertFalse(ack.isFailure());
    }

    @Test
    void failure_rejectsNullAndBlankErrorMsg() {
        assertThrows(
            NullPointerException.class,
            () -> TransitionAck.failure(
                OP_ID,
                TOPIC,
                TransitionType.TOPIC_FAILOVER,
                TransitionParticipation.INVOLVED,
                HOST,
                TransitionStage.PREPARE,
                null
            )
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> TransitionAck.failure(
                OP_ID,
                TOPIC,
                TransitionType.TOPIC_FAILOVER,
                TransitionParticipation.INVOLVED,
                HOST,
                TransitionStage.PREPARE,
                ""
            )
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> TransitionAck.failure(
                OP_ID,
                TOPIC,
                TransitionType.TOPIC_FAILOVER,
                TransitionParticipation.INVOLVED,
                HOST,
                TransitionStage.PREPARE,
                "   "
            )
        );
    }

    private static void assertDerivedKeysAbsent(String json) throws Exception {
        JsonNode node = JsonMapper.getMapper().readTree(json);
        assertFalse(node.has("success"), () -> "unexpected success key in: " + json);
        assertFalse(node.has("failure"), () -> "unexpected failure key in: " + json);
    }
}
