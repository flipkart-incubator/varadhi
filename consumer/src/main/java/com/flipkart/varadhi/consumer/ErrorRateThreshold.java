package com.flipkart.varadhi.consumer;

/**
 * Defines the threshold above which the rate limiter should start throttling processing. It can be static or dynamic.
 * It is defined in terms of the error rate per sec.
 */
public interface ErrorRateThreshold {

    float getThreshold();

    /**
     * Marker interface for dynamic error rate threshold.
     */
    interface Dynamic extends ErrorRateThreshold {

        void addListener(ErrorThresholdChangeListener listener);

        void removeListener(ErrorThresholdChangeListener listener);
    }

    interface ErrorThresholdChangeListener {
        void onThresholdChange(float newThreshold);
    }
}
