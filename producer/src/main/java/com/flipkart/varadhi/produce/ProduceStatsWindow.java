package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.entities.TopicProduceStats;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

final class ProduceStatsWindow {
    private static final long WINDOW_MILLIS = 60_000;
    private static final int WINDOW_COUNT = 5;

    private final LongSupplier clockMillis;
    private final long[] successes = new long[WINDOW_COUNT];
    private final long[] failures = new long[WINDOW_COUNT];
    private long currentStart;

    ProduceStatsWindow(LongSupplier clockMillis) {
        this.clockMillis = clockMillis;
        currentStart = windowStart(clockMillis.getAsLong());
    }

    synchronized void recordSuccess() {
        advance();
        successes[index(currentStart)]++;
    }

    synchronized void recordFailure() {
        advance();
        failures[index(currentStart)]++;
    }

    synchronized TopicProduceStats snapshot() {
        advance();
        Map<Long, Long> successCounts = new LinkedHashMap<>();
        Map<Long, Long> failureCounts = new LinkedHashMap<>();
        for (int offset = WINDOW_COUNT - 1; offset >= 0; offset--) {
            long start = currentStart - offset * WINDOW_MILLIS;
            int index = index(start);
            successCounts.put(start, successes[index]);
            failureCounts.put(start, failures[index]);
        }
        return new TopicProduceStats(Map.copyOf(successCounts), Map.copyOf(failureCounts));
    }

    private void advance() {
        long now = windowStart(clockMillis.getAsLong());
        if (now <= currentStart) {
            return;
        }
        if (now - currentStart >= WINDOW_COUNT * WINDOW_MILLIS) {
            java.util.Arrays.fill(successes, 0);
            java.util.Arrays.fill(failures, 0);
        } else {
            for (long start = currentStart + WINDOW_MILLIS; start <= now; start += WINDOW_MILLIS) {
                int index = index(start);
                successes[index] = 0;
                failures[index] = 0;
            }
        }
        currentStart = now;
    }

    private int index(long start) {
        return (int)Math.floorMod(start / WINDOW_MILLIS, WINDOW_COUNT);
    }

    private static long windowStart(long millis) {
        return Math.floorDiv(millis, WINDOW_MILLIS) * WINDOW_MILLIS;
    }
}
