package com.flipkart.varadhi.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Ticker;

public class MockTicker extends Ticker {

    private final AtomicLong nanos;

    public MockTicker(long nanos) {
        this.nanos = new AtomicLong(nanos);
    }

    public MockTicker(long time, TimeUnit unit) {
        this.nanos = new AtomicLong(unit.toNanos(time));
    }

    public void advanceNanos(long nanos) {
        this.nanos.addAndGet(nanos);
    }

    public void setNanos(long nanos) {
        this.nanos.set(nanos);
    }

    public void advance(long time, TimeUnit unit) {
        advanceNanos(unit.toNanos(time));
    }

    public void set(long time, TimeUnit unit) {
        setNanos(unit.toNanos(time));
    }

    @Override
    public long read() {
        return nanos.get();
    }
}
