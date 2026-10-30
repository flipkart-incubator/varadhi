package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.common.MockTicker;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProduceStatsWindowTest {
    @Test
    void keepsFiveWindowsAndRollsCounts() {
        MockTicker ticker = new MockTicker(0, TimeUnit.MILLISECONDS);
        ProduceStatsWindow stats = new ProduceStatsWindow(() -> ticker.read() / 1_000_000);

        stats.recordSuccess();
        stats.recordFailure();
        assertEquals(1L, stats.snapshot().successCounts().get(0L));
        assertEquals(1L, stats.snapshot().failureCounts().get(0L));

        ticker.advance(60, TimeUnit.SECONDS);
        stats.recordSuccess();
        assertEquals(1L, stats.snapshot().successCounts().get(0L));
        assertEquals(1L, stats.snapshot().successCounts().get(60_000L));
        assertEquals(5, stats.snapshot().successCounts().size());
    }
}
