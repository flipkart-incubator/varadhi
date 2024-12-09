package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.ThresholdProvider;
import com.google.common.base.Ticker;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A sliding window based error rate threshold.
 * - Window size: dictates the averaging window size.
 * - Tick rate: The rate at which the window slides. Tick rate <= window size.
 *
 * @Threadsafe
 */
@Slf4j
public class SlidingWindowThresholdProvider implements ThresholdProvider.Dynamic, AutoCloseable {

    private final ScheduledExecutorService scheduler;
    private final Ticker ticker;
    private final float pctErrorThreshold;
    private final long tickMs;
    private final int ticksInWindow;
    private final int totalTicks;

    /**
     * The ticks array is used to store the number of data points in each tick. This array acts as circular queue.
     * Can be updated by arbitrary threads.
     */
    private final AtomicIntegerArray ticks;


    private long windowBeginTick;

    /**
     * total data points from the last window. It is the summation of the values in the ticks array between windowBeginIdx and windowEndIdx.
     * Changing windowBeginIdx and windowEndIdx should change the total data points.
     */
    private int totalDatapoints;

    // listeners
    private final List<ThresholdChangeListener> listeners = new CopyOnWriteArrayList<>();

    private ScheduledFuture<?> thresholdUpdater;

    public SlidingWindowThresholdProvider(
            ScheduledExecutorService scheduler, Ticker ticker, int windowSizeMs, int tickMs, float pctErrorThreshold
    ) {
        this.scheduler = scheduler;
        this.ticker = ticker;
        this.pctErrorThreshold = pctErrorThreshold;
        this.tickMs = tickMs;

        if (windowSizeMs % tickMs != 0) {
            throw new IllegalArgumentException("Window size should be a multiple of tick rate");
        }
        this.ticksInWindow = windowSizeMs / tickMs;

        // We are using 2 times, so that we can track 2 window worth of datapoints.
        this.totalTicks = 2 * ticksInWindow;
        this.ticks = new AtomicIntegerArray(totalTicks);
        this.totalDatapoints = 0;
        this.windowBeginTick = currentTick() - ticksInWindow;
        thresholdUpdater = scheduleTask();
    }

    /**
     * Add a new datapoint to the current tick. If the tick has changed, then move the window adjusting the total data points.
     * Can be called by arbitrary threads.
     */
    @Override
    public void mark() {
        long currentTick = currentTick();
        // mark
        int idx = (int) (currentTick % totalTicks);
        ticks.incrementAndGet(idx);
    }

    long currentTick() {
        return (ticker.read() / 1_000_000) / tickMs;
    }

    @Override
    public float getThreshold() {
        return totalDatapoints * (pctErrorThreshold / 100.0f) / (ticksInWindow * tickMs / 1000.0f);
    }

    @Override
    public void addListener(ThresholdChangeListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(ThresholdChangeListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void close() throws Exception {
        if (thresholdUpdater != null) {
            thresholdUpdater.cancel(true);
        }
    }

    private ScheduledFuture<?> scheduleTask() {
        // schedule it at double the tick rate, so that we "don't miss" any tick changes.
        return scheduler.scheduleAtFixedRate(() -> {
            boolean moved = moveWindow();
            if (moved) {
                notifyListeners(getThreshold());
            }
        }, 0, tickMs / 2, TimeUnit.MILLISECONDS);
    }

    private void notifyListeners(float threshold) {
        for (ThresholdChangeListener listener : listeners) {
            listener.onThresholdChange(threshold);
        }
    }

    /**
     * Move the window so that the currentTick - ticksInWindow is the new window beginning tick.
     * Decrement all tick values are now too old, and add new tick values that got added in the window.
     */
    synchronized boolean moveWindow() {
        long newWindowBeginTick = currentTick() - ticksInWindow;
        if (newWindowBeginTick == windowBeginTick) {
            return false;
        }

        for (long i = windowBeginTick; i < newWindowBeginTick; ++i) {
            int beginIdx = (int) (i % totalTicks);
            int endIdx = (int) ((i + ticksInWindow) % totalTicks);
            totalDatapoints += (ticks.get(endIdx) - ticks.getAndSet(beginIdx, 0));
        }

        windowBeginTick = newWindowBeginTick;
        return true;
    }
}
