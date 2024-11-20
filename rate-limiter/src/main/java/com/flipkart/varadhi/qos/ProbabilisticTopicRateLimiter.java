package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.RateLimiterType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class ProbabilisticTopicRateLimiter implements FactorRateLimiter {

    private final String topic;
    private final RateLimiterType type;
    private volatile double suppressionFactor;

    public ProbabilisticTopicRateLimiter(String topic, RateLimiterType type) {
        this.topic = topic;
        this.type = type;
        this.suppressionFactor = 0;
    }

    @Override
    public Boolean addTrafficData(Long value) {
        // generate random number between 0 and 1
        return Math.random() > suppressionFactor;
    }

    @Override
    public void updateSuppressionFactor(double suppressionFactor) {
        this.suppressionFactor = suppressionFactor;
    }
}
