package com.flipkart.varadhi.consumer;

import com.google.common.base.Ticker;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * A sliding window based error rate threshold.
 * - Window size: dictates the averaging window size.
 * - Tick rate: The rate at which the window slides. Tick rate <= window size.
 *
 * TODO: thread safety
 */
public class SlidingErrorRateThreshold implements ErrorRateThreshold.Dynamic, AutoCloseable {

    private final ScheduledExecutorService scheduler;
    private final Ticker ticker;
    private final float pctErrorThreshold;
    private final long windowSizeMs;
    private final long tickRateMs;
    private final int ticksInWindow;
    private final int totalTicks;

    /**
     * The ticks array is used to store the number of data points in each tick. This array acts as circular queue.
     */
    private final int[] ticks;


    private long windowBeginTick;

    /**
     * The last window beginning index in the ticks array. Inclusive.
     */
    int windowBeginIdx() {
        return (int) (windowBeginTick % totalTicks);
    }

    /**
     * The last window ending index in the ticks array. Inclusive.
     *
     * @return
     */
    int windowEndIdx() {
        return (int) ((windowBeginTick + ticksInWindow - 1) % totalTicks);
    }

    /**
     * total data points from the last window. It is the summation of the values in the ticks array between windowBeginIdx and windowEndIdx.
     * Changing windowBeginIdx and windowEndIdx should change the total data points.
     */
    private int totalDatapoints;

    // listeners
    private final List<ErrorThresholdChangeListener> listeners = new CopyOnWriteArrayList<>();

    private ScheduledFuture<?> thresholdUpdater;

    public SlidingErrorRateThreshold(
            ScheduledExecutorService scheduler, Ticker ticker, int windowSizeMs, int tickRateMs, float pctErrorThreshold
    ) {
        this.scheduler = scheduler;
        this.ticker = ticker;
        this.pctErrorThreshold = pctErrorThreshold;
        this.windowSizeMs = windowSizeMs;
        this.tickRateMs = tickRateMs;

        if (windowSizeMs % tickRateMs != 0) {
            throw new IllegalArgumentException("Window size should be a multiple of tick rate");
        }
        this.ticksInWindow = windowSizeMs / tickRateMs;

        // We are using 2 times, so that we can track 2 window worth of datapoints.
        this.totalTicks = 2 * ticksInWindow;
        this.ticks = new int[totalTicks];
        this.totalDatapoints = 0;
        this.windowBeginTick = currentWindowBeginTick();
        scheduleTask();
    }

    /**
     * Add a new datapoint to the current tick. If the tick has changed, then move the window adjusting the total data points.
     */
    public void mark() {
        long currentTick = currentWindowBeginTick();
        // mark
        int idx = (int) (currentTick % totalTicks);
        ticks[idx]++;
    }

    long currentWindowBeginTick() {
        return ((ticker.read() / 1_000_000) - windowSizeMs) / tickRateMs;
    }

    @Override
    public float getThreshold() {
        return totalDatapoints * pctErrorThreshold / 100.0f;
    }

    @Override
    public void addListener(ErrorThresholdChangeListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(ErrorThresholdChangeListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void close() throws Exception {
        if (thresholdUpdater != null) {
            thresholdUpdater.cancel(true);
        }
    }

    private void scheduleTask() {
        // schedule it at double the tick rate, so that we "don't miss" any tick changes.
        thresholdUpdater = scheduler.scheduleAtFixedRate(() -> {
            boolean moved = moveWindow();
            if (moved) {
                notifyListeners(getThreshold());
            }
        }, 0, tickRateMs / 2, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void notifyListeners(float threshold) {
        for (ErrorThresholdChangeListener listener : listeners) {
            listener.onThresholdChange(threshold);
        }
    }

    /**
     * Move the window so that the currentTick - ticksInWindow is the new window beginning tick.
     * Decrement all tick values are now too old, and add new tick values that got added in the window.
     */
    boolean moveWindow() {
        long currentTick = currentWindowBeginTick();
        if (currentTick == windowBeginTick) {
            return false;
        }

        long newWindowBeginTick = currentTick - ticksInWindow;

        // decrement the old ticks
        for (long i = windowBeginTick; i < newWindowBeginTick; ++i) {
            int beginIdx = (int) (i % totalTicks);
            int endIdx = (int) ((i + ticksInWindow) % totalTicks);
            totalDatapoints += (ticks[endIdx] - ticks[beginIdx]);
            ticks[beginIdx] = 0;
        }

        windowBeginTick = newWindowBeginTick;
        return true;
    }
}
