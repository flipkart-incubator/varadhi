package com.flipkart.varadhi.qos;

import com.google.common.util.concurrent.AtomicDouble;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicRateLimiter implements RateLimiter {

    @Getter
    private final String topic;
    @Getter
    private final String name;
    private volatile double suppressionFactor;
    private AtomicDouble lastObserved;
    private AtomicDouble currentObserved;

    public TopicRateLimiter(String topic, String name) {
        this.topic = topic;
        this.name = name;
        this.suppressionFactor = 0;
        lastObserved = new AtomicDouble(0.0);
        currentObserved = new AtomicDouble(0.0);
    }

    @Override
    public boolean isAllowed(Double value) {
        currentObserved.addAndGet(value);
        if(suppressionFactor == 0) {
            return true;
        }
        return currentObserved.get() <= lastObserved.get()*(1-suppressionFactor);
    }

    public void setSuppressionFactor(double suppressionFactor) {
        lastObserved.set(currentObserved.get());
        // reset current observed
        currentObserved.set(0.0);
        this.suppressionFactor = suppressionFactor;
    }
}
