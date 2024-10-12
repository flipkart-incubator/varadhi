package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.RateLimiterType;

import java.util.concurrent.atomic.LongAdder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicRateLimiter implements RateLimiter {

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
    public boolean isAllowed(long value) {
        this.currentObserved.add(value);
        // todo(rl): allows spikes, need to consider a better way to handle spikes
        if (this.suppressionFactor == 0) {
            return true;
        }
        return this.currentObserved.longValue() <= this.lastObserved * (1 - this.suppressionFactor);
    }

    public void updateSuppressionFactor(double suppressionFactor) {
        this.lastObserved = this.currentObserved.longValue();
        // remove last recorded value
        this.currentObserved.add(-lastObserved);
        this.suppressionFactor = suppressionFactor;
    }
}
