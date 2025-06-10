package com.flipkart.varadhi.web.core.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

import java.util.function.Function;
import java.util.function.Supplier;

public class ApiMetrics {

    private final Timer apiLatency;

    /*
        4 entries for 2xx, 3xx, 4xx, 5xx. Status code is under our control, so we can be sure that we are only using
        these sets of response codes.
     */
    private final Counter[] responseCounters = new Counter[4];

    public ApiMetrics(Supplier<Timer> latencyTimer) {
        this.apiLatency = latencyTimer.get();
    }

    public Timer getApiLatencyTimer() {
        return apiLatency;
    }

    public Counter getResponseCounter(int responseCode, Function<Integer, Counter> supplier) {
        int index = responseCode / 100 - 2; // 2xx -> 0, 3xx -> 1, 4xx -> 2, 5xx -> 3
        if (index < 0 || index >= responseCounters.length) {
            throw new IllegalArgumentException("Invalid response code: " + responseCode);
        }
        // safe, because ref update is safe and the actual headache of concurrency & metric de-duplication needs to be
        // handled by the supplier.
        if (responseCounters[index] == null) {
            responseCounters[index] = supplier.apply(responseCode);
        }
        return responseCounters[index];
    }
}
