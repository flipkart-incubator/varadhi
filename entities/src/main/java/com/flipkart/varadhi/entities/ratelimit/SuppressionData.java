package com.flipkart.varadhi.entities.ratelimit;

import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Suppression data for rate limiting. Stores suppression factor for each topic.
 * @param <T> type of suppression factor (e.g. Float)
 */
@Getter
@Setter
public class SuppressionData<T> {
    Map<String, T> suppressionFactor;

    public SuppressionData() {
        this.suppressionFactor = new ConcurrentHashMap<>();
    }
}
