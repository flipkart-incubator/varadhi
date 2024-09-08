package com.flipkart.varadhi.utils.weights;

public interface WeightFunction {
    float applyWeight(long time, long currentTime, long windowSize);
}
