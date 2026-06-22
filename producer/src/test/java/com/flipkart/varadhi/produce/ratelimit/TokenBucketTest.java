package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.common.MockTicker;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketTest {

    private static final long NS_PER_SEC = 1_000_000_000L;

    private final MockTicker ticker = new MockTicker(0L);

    @Test
    void hasPositiveCredit_StartsFullAtCapacity() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 10);

        assertEquals(10L, bucket.capacity());
        assertTrue(bucket.hasPositiveCredit());
        assertEquals(10L, bucket.tokensForTest());
    }

    @Test
    void debit_ExhaustsThenRefillsOverTime() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 2);

        bucket.debit(2);
        assertFalse(bucket.hasPositiveCredit());

        ticker.advance(500, TimeUnit.MILLISECONDS);
        assertTrue(bucket.hasPositiveCredit());

        bucket.debit(2);
        assertFalse(bucket.hasPositiveCredit());
    }

    @Test
    void hasPositiveCredit_CreditBasedAdmission_MayGoNegativeOnOversizedDebit() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 1);
        bucket.debit(1);
        assertFalse(bucket.hasPositiveCredit());

        bucket.debit(100);
        assertTrue(bucket.tokensForTest() < 0L);
    }

    @Test
    void updateRate_ResizesCapacityAndRefillRate() {
        TokenBucket bucket = new TokenBucket(ticker, 2, 5);
        assertEquals(10L, bucket.capacity());

        bucket.updateRate(20);
        assertEquals(40L, bucket.capacity());
    }

    @Test
    void refill_CapsAtCapacity() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 5);
        bucket.debit(5);
        ticker.advance(5, TimeUnit.SECONDS);

        assertEquals(5L, bucket.tokensForTest());
        ticker.advance(5, TimeUnit.SECONDS);
        assertEquals(5L, bucket.tokensForTest());
    }

    @Test
    void refill_SubSecondElapsedUsesIntegerNanos() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 2);
        bucket.debit(2);
        assertFalse(bucket.hasPositiveCredit());

        ticker.advance(500, TimeUnit.MILLISECONDS);
        assertTrue(bucket.hasPositiveCredit());
        assertEquals(1L, bucket.tokensForTest());
    }

    @Test
    void refill_HighByteRateDoesNotOverflow() {
        // 10 GB/s exceeds the ~9.2e9 threshold where remainderNanos * rate would overflow a long.
        long highRate = 10_000_000_000L;
        TokenBucket bucket = new TokenBucket(ticker, 1, highRate);
        bucket.debit(highRate);
        assertFalse(bucket.hasPositiveCredit());

        ticker.advanceNanos(NS_PER_SEC - 1L);
        long refilled = bucket.tokensForTest();
        assertTrue(refilled > 0L, "refill must stay positive, not overflow negative");
        assertEquals((NS_PER_SEC - 1L) * (highRate / NS_PER_SEC), refilled);
    }

    @Test
    void updateRate_OverflowingCapacityClampsFailOpen() {
        // rate * windowSecs overflows long; capacity must clamp to MAX_VALUE, not wrap negative.
        TokenBucket bucket = new TokenBucket(ticker, 4, Long.MAX_VALUE / 2);
        assertEquals(Long.MAX_VALUE, bucket.capacity());
        assertTrue(bucket.hasPositiveCredit());
    }

    @Test
    void zeroRate_NeverAdmits() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 0);
        assertEquals(0L, bucket.capacity());
        assertFalse(bucket.hasPositiveCredit());

        ticker.advance(10, TimeUnit.SECONDS);
        assertFalse(bucket.hasPositiveCredit());
    }

    @Test
    void updateRate_Downshift_ReclampsTokensOnNextRefill() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 10);
        assertEquals(10L, bucket.tokensForTest());

        bucket.updateRate(2);
        // Documented transient: tokens above the new capacity persist until the next timed refill.
        assertEquals(10L, bucket.tokensForTest());

        ticker.advanceNanos(1L);
        assertEquals(2L, bucket.tokensForTest());
    }

    @Test
    void debit_ConcurrentDebitsHaveNoLostUpdates() throws InterruptedException {
        int threads = 8;
        int debitsPerThread = 10_000;
        long total = (long)threads * debitsPerThread;
        // Clock stays frozen at 0, so no refill happens; only CAS debits mutate state.
        TokenBucket bucket = new TokenBucket(ticker, 1, total);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.execute(() -> {
                try {
                    start.await();
                    for (int i = 0; i < debitsPerThread; i++) {
                        bucket.debit(1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdownNow();

        assertEquals(0L, bucket.tokensForTest());
    }

    @Test
    void debit_StaleNowDoesNotRegressLastNano() {
        TokenBucket bucket = new TokenBucket(ticker, 1, 10);
        bucket.debit(1, 200L);
        assertEquals(200L, bucket.lastNanoForTest());

        bucket.debit(1, 100L);
        assertEquals(200L, bucket.lastNanoForTest());
    }

    @Test
    void refill_AtMaxCapacityDoesNotWrapNegative() {
        TokenBucket bucket = new TokenBucket(ticker, 4, Long.MAX_VALUE / 2);
        assertEquals(Long.MAX_VALUE, bucket.capacity());

        bucket.debit(1_000_000L);
        ticker.advance(10, TimeUnit.SECONDS);

        assertTrue(bucket.tokensForTest() >= 0L);
        assertEquals(Long.MAX_VALUE, bucket.tokensForTest());
    }
}
