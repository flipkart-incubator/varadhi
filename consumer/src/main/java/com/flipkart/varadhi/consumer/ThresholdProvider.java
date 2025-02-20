package com.flipkart.varadhi.consumer;

/**
 * Defines the threshold above which the rate limiter should start throttling processing. It can be static or dynamic.
 * It is defined in terms of the error rate per sec.
 */
public interface ThresholdProvider {

    float getThreshold();

    /**
     * Marker interface for dynamic error rate threshold.
     */
    interface Dynamic extends ThresholdProvider {

        void mark();

        void addListener(ThresholdChangeListener listener);

        void removeListener(ThresholdChangeListener listener);
    }


    interface ThresholdChangeListener {
        void onThresholdChange(float newThreshold);
    }
}
