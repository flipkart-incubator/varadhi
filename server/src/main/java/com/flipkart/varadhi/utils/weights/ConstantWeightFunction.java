package com.flipkart.varadhi.utils.weights;

public class ConstantWeightFunction implements WeightFunction {
    @Override
    public float applyWeight(long time, long currentTime, long windowSize) {
        if(time < (currentTime - windowSize)) {
            return 0.0f;
        }
        return 1.0f;
    }
}
