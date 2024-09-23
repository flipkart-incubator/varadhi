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
    private volatile float suppressionFactor;
    private AtomicDouble lastObserved;
    private AtomicDouble currentObserved;

    public TopicRateLimiter(String topic, String name) {
        this.topic = topic;
        this.name = name;
        this.suppressionFactor = 0f;
        lastObserved = new AtomicDouble(0.0);
        currentObserved = new AtomicDouble(0.0);
    }

    @Override
    public boolean isAllowed(Double value) {
        currentObserved.addAndGet(value);
        if(suppressionFactor == 0) {
            return true;
        }
        log.info("Current observed: {}, Last observed: {}", currentObserved, lastObserved);
        log.info("Suppression factor: {}", suppressionFactor);
        log.info("Allowed: {}", currentObserved.get() <= lastObserved.get()*(1-suppressionFactor));
        return currentObserved.get() <= lastObserved.get()*(1-suppressionFactor);
    }

    public void setSuppressionFactor(float suppressionFactor) {
        log.info("BEFORE SETTING SUPPRESSION FACTOR");
        log.info("current observed: {}", currentObserved);
        log.info("last observed: {}", lastObserved);
        lastObserved.set(currentObserved.get());
        // reset current observed
        currentObserved.set(0.0);
        this.suppressionFactor = suppressionFactor;
        log.info("AFTER SETTING SUPPRESSION FACTOR");
        log.info("current observed: {}", currentObserved);
        log.info("last observed: {}", lastObserved);

    }
}
