package com.flipkart.varadhi.entities.ratelimit;

import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class SuppressionData<T> {
    Map<String, T> suppressionFactor;
    RateLimitReason reason;

    public SuppressionData() {
        this.suppressionFactor = new ConcurrentHashMap<>();
    }
}
