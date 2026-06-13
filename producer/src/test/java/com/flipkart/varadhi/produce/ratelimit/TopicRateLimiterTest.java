package com.flipkart.varadhi.produce.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicRateLimiterTest {

    private final AtomicLong virtualNanos = new AtomicLong(0L);

    @Test
    void tryAcquire_DebitsBothBucketsOnAllow() {
        TopicRateLimiter limiter = newLimiter(new PerPodTopicQuota(2, 200));

        assertTrue(limiter.tryAcquire(100));
        assertTrue(limiter.tryAcquire(100));
        assertFalse(limiter.tryAcquire(1));
    }

    @Test
    void tryAcquire_BothOrNeither_WhenBytesEmptyDoesNotDebitQps() {
        TopicRateLimiter limiter = newLimiter(new PerPodTopicQuota(3, 1));
        assertTrue(limiter.tryAcquire(1));

        assertFalse(limiter.tryAcquire(1));

        virtualNanos.addAndGet(1_000_000_000L);
        assertTrue(limiter.tryAcquire(1));
        virtualNanos.addAndGet(1_000_000_000L);
        assertTrue(limiter.tryAcquire(1));
        assertFalse(limiter.tryAcquire(1));
    }

    @Test
    void tryAcquire_RejectsWithoutDebitingWhenBucketsEmpty() {
        TopicRateLimiter limiter = newLimiter(new PerPodTopicQuota(1, 1));
        assertTrue(limiter.tryAcquire(1));

        assertFalse(limiter.tryAcquire(1));

        virtualNanos.addAndGet(1_000_000_000L);
        assertTrue(limiter.tryAcquire(1));
    }

    @Test
    void tryAcquire_NegativeMessageBytesDoesNotCreditBytesBucket() {
        TopicRateLimiter limiter = newLimiter(new PerPodTopicQuota(100, 1000));
        assertTrue(limiter.tryAcquire(900));

        // Clamped to zero cost: must not refund the bytes bucket back toward capacity.
        assertTrue(limiter.tryAcquire(-1000));

        assertTrue(limiter.tryAcquire(100));
        assertFalse(limiter.tryAcquire(1));
    }

    @Test
    void applyQuota_ResizesLiveBuckets() {
        TopicRateLimiter limiter = newLimiter(new PerPodTopicQuota(1, 50));
        assertTrue(limiter.tryAcquire(50));

        limiter.applyQuota(new PerPodTopicQuota(100, 100_000));
        virtualNanos.addAndGet(1_000_000_000L);
        assertTrue(limiter.tryAcquire(50));
    }

    private TopicRateLimiter newLimiter(PerPodTopicQuota quota) {
        return new TopicRateLimiter(virtualNanos::get, 1, quota);
    }
}
