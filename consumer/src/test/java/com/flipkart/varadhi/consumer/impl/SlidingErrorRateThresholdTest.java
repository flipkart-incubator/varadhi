package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.MockTicker;
import com.google.common.base.Ticker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;

class SlidingErrorRateThresholdTest {

    private final ScheduledExecutorService noopScheduler = mock(ScheduledExecutorService.class);
    private final ScheduledExecutorService defaultScheduler = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void testManualMoveWindow() throws Exception {
        MockTicker ticker = new MockTicker(System.currentTimeMillis() / 10 * 10_000_000);
        try (var rt = new SlidingErrorRateThreshold(noopScheduler, ticker, 10_000, 1_000, 50.0f)) {
            Assertions.assertEquals(0.0f, rt.getThreshold(), 0.01f);

            // mark for full window. the threshold should increase by 5 at every sec. after 10 sec it should say 50.
            for (int i = 0; i < 10; i++) {
                IntStream.range(0, 100).forEach(_i -> rt.mark());
                ticker.advance(1, TimeUnit.SECONDS);
                rt.moveWindow();
                Assertions.assertEquals(
                        50.0f * (0.1f * (i + 1)), rt.getThreshold(), 0.01f, "unexpected rate in iteration: " + i);
            }

            // now, any more marks at the same rate should just say 50.
            for (int i = 0; i < 10; i++) {
                IntStream.range(0, 100).forEach(_i -> rt.mark());
                ticker.advance(1, TimeUnit.SECONDS);
                rt.moveWindow();
                Assertions.assertEquals(50.0f, rt.getThreshold(), 0.01f, "unexpected rate in iteration: " + i);
            }
        }
    }

    @Test
    public void testRateComputationIsCorrect() throws Exception {
        try (var rt = new SlidingErrorRateThreshold(defaultScheduler, Ticker.systemTicker(), 1_000, 10, 10.0f)) {
            ScheduledFuture<?> eventEmitter = defaultScheduler.scheduleAtFixedRate(rt::mark, 0, 1, TimeUnit.MILLISECONDS);

            Thread.sleep(1000);
            eventEmitter.cancel(true);
            // emiiter is 1000 qps, but threshold is 10%.
            Assertions.assertEquals(100.0f, rt.getThreshold(), 5.0f);

            eventEmitter = defaultScheduler.scheduleAtFixedRate(rt::mark, 0, 2, TimeUnit.MILLISECONDS);

            Thread.sleep(1000);
            eventEmitter.cancel(true);
            // emiiter is 1000 qps, but threshold is 10%.
            Assertions.assertEquals(50.0f, rt.getThreshold(), 2.5f);
        }
    }
}
