package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.RateLimiterType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ProbabilisticTopicRateLimiterTests {

    private ProbabilisticTopicRateLimiter rateLimiter;
    private final String DEFAULT_TOPIC = "topic1";
    private final RateLimiterType type = RateLimiterType.THROUGHPUT;

    @BeforeEach
    public void setUp() {
        rateLimiter = new ProbabilisticTopicRateLimiter(DEFAULT_TOPIC, type);
    }

    @Test
    public void testInitialization() {
        assertEquals(DEFAULT_TOPIC, rateLimiter.getTopic());
        assertEquals(type, rateLimiter.getType());
        assertEquals(0, rateLimiter.getSuppressionFactor());
    }

    @Test
    public void testUpdateSuppressionFactor() {
        rateLimiter.updateSuppressionFactor(0.7);
        assertEquals(0.7, rateLimiter.getSuppressionFactor());
    }

    @Test
    public void testZeroFactor() {
        rateLimiter.updateSuppressionFactor(0);
        assertTrue(rateLimiter.addTrafficData(100L));
    }

    @Test
    public void testOneFactor() {
        rateLimiter.updateSuppressionFactor(1);
        assertFalse(rateLimiter.addTrafficData(100L));
    }

    @Test
    public void testIntermediateFactor() {
        rateLimiter.updateSuppressionFactor(0.5);
        int trueCount = 0;
        int falseCount = 0;
        for (int i = 0; i < 1000; i++) {
            if (rateLimiter.addTrafficData(100L)) {
                trueCount++;
            } else {
                falseCount++;
            }
        }
        assertTrue(trueCount > 0);
        assertTrue(falseCount > 0);
    }
}
