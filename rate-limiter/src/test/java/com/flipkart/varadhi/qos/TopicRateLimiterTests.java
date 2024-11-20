package com.flipkart.varadhi.qos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flipkart.varadhi.qos.entity.RateLimiterType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TopicRateLimiterTests {

    private TopicRateLimiter rateLimiter;
    private final String topic = "test-topic";
    private final RateLimiterType type = RateLimiterType.THROUGHPUT;

    @BeforeEach
    public void setUp() {
        rateLimiter = new TopicRateLimiter(topic, type);
    }

    @Test
    public void testInitialization() {
        assertEquals(topic, rateLimiter.getTopic());
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
        assertTrue(rateLimiter.addTrafficData(100L));
        rateLimiter.updateSuppressionFactor(0);
        assertTrue(rateLimiter.addTrafficData(100L));
    }

    @Test
    public void testOneFactor() {
        assertTrue(rateLimiter.addTrafficData(100L));
        rateLimiter.updateSuppressionFactor(1);
        assertFalse(rateLimiter.addTrafficData(100L));
    }

    @Test
    public void testIntermediateFactor() {
        rateLimiter.addTrafficData(100L);
        rateLimiter.updateSuppressionFactor(0.5);
        rateLimiter.addTrafficData(100L);
        assertFalse(rateLimiter.addTrafficData(100L));
    }

    @Test
    public void testWithNoFactor() {
        assertTrue(rateLimiter.addTrafficData(100L));
        assertTrue(rateLimiter.addTrafficData(200L));
    }

    @Test
    public void testWithFactor() {
        assertTrue(rateLimiter.addTrafficData(200L));
        rateLimiter.updateSuppressionFactor(0.5);
        assertTrue(rateLimiter.addTrafficData(100L));
        assertFalse(rateLimiter.addTrafficData(200L));
    }

    @Test
    public void testChangingFactor() {
        rateLimiter.updateSuppressionFactor(0.3);
        rateLimiter.addTrafficData(100L);
        assertTrue(rateLimiter.addTrafficData(50L));
        rateLimiter.updateSuppressionFactor(0.7);
        assertFalse(rateLimiter.addTrafficData(100L));
    }
}
