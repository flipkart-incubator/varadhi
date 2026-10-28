package com.flipkart.varadhi.entities.cluster.transition.failover;

import com.flipkart.varadhi.entities.cluster.Operation;
import com.flipkart.varadhi.entities.JsonMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TopicFailoverOperationTest {

    @Test
    void of_retry_andReloadPreserveAudit() {
        TopicFailoverOperation op = TopicFailoverOperation.of("proj.topic", "user");
        assertEquals("user", op.getRequestedBy());
        assertEquals(Operation.State.IN_PROGRESS, op.getState());

        TopicFailoverOperation retry = op.nextRetry();
        assertEquals(1, retry.getRetryAttempt());
        assertEquals("user", retry.getRequestedBy());
        assertEquals(op.getCreatedAt(), retry.getCreatedAt());
        assertEquals(Operation.State.IN_PROGRESS, retry.getState());
        assertNull(retry.getErrorMsg());

        op.markCompleted();
        TopicFailoverOperation restored = JsonMapper.jsonDeserialize(
            JsonMapper.jsonSerialize(op),
            TopicFailoverOperation.class
        );
        assertEquals(Operation.State.COMPLETED, restored.getState());
        assertEquals("user", restored.getRequestedBy());
        assertEquals(op.getCreatedAt(), restored.getCreatedAt());

        TopicFailoverOperation stored = TopicFailoverOperation.of("proj.topic", "other");
        stored.applyProgressFrom(op);
        assertEquals(Operation.State.COMPLETED, stored.getState());
        assertEquals(op.getUpdatedAt(), stored.getUpdatedAt());
    }
}
