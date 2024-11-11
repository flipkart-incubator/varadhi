package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.RateLimiterType;

import java.util.concurrent.atomic.LongAdder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicRateLimiter implements FactorRateLimiter {

    @Getter
    private final String topic;
    @Getter
    private final RateLimiterType type;
    @Getter
    private volatile double suppressionFactor;
    private long lastObserved;
    private final LongAdder currentObserved;

    public TopicRateLimiter(String topic, RateLimiterType type) {
        this.topic = topic;
        this.type = type;
        this.suppressionFactor = 0;
        this.lastObserved = 0L;
        this.currentObserved = new LongAdder();
    }

    @Override
    public Boolean addTrafficData(Long value) {
        currentObserved.add(value);
        // todo(rl): allows spikes, need to consider a better way to handle spikes
        if (suppressionFactor == 0) {
            return true;
        }
        return currentObserved.longValue() <= lastObserved * (1 - suppressionFactor);
    }

    @Override
    public void updateSuppressionFactor(double suppressionFactor) {
        lastObserved = currentObserved.longValue();
        // remove last recorded value
        currentObserved.add(-lastObserved);
        this.suppressionFactor = suppressionFactor;
    }
}
