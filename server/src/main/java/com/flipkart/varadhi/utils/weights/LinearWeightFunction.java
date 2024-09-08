package com.flipkart.varadhi.utils.weights;

public class LinearWeightFunction implements WeightFunction {
    @Override
    public float applyWeight(long time, long currentTime, long windowSize) {
        if(time < (currentTime - windowSize)) {
            return 0.0f;
        }
        return (float) (time - (currentTime - windowSize)) / windowSize;
    }
}
