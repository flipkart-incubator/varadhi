package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.entities.ProduceStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceResultTest {

    @Test
    void ofThrottled_ReturnsThrottledStatus() {
        ProduceResult result = ProduceResult.ofThrottled("msg-1");

        assertEquals(ProduceStatus.Throttled, result.getProduceStatus());
        assertEquals("msg-1", result.getMessageId());
        assertTrue(result.isThrottled());
    }
}
