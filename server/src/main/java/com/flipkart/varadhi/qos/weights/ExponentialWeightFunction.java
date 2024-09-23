package com.flipkart.varadhi.qos.weights;

public class ExponentialWeightFunction implements WeightFunction {
    private final float k;

    public ExponentialWeightFunction(float k) {
        this.k = k;
    }

    @Override
    public float applyWeight(long time, long currentTime, long windowSize) {
        if(time < (currentTime - windowSize)) {
            return 0.0f;
        }
        return 1.0f - (float) Math.exp(-k * (time - (currentTime - windowSize)) / windowSize);
    }
}
