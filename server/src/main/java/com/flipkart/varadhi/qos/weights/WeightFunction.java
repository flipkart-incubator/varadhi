package com.flipkart.varadhi.qos.weights;

public interface WeightFunction {
    float applyWeight(long time, long currentTime, long windowSize);
}
