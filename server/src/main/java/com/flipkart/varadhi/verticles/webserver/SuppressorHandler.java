package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.ratelimit.SuppressionData;

public class SuppressorHandler {
    SuppressionData<Float> suppressionData;

    public SuppressorHandler() {
        this.suppressionData = new SuppressionData<>();
    }

    public float getSuppressionFactor(String topic) {
        return suppressionData.getSuppressionFactor().getOrDefault(topic, 0f);
    }

    public void handle(ClusterMessage message) {
        this.suppressionData = message.getData(SuppressionData.class);
        suppressionData.getSuppressionFactor().forEach((topic, factor) -> {
//            this.suppressionData.getSuppressionFactor().put(topic, factor);
            System.out.printf("Got topic: %s, factor: %s%n", topic, factor);
        });
    }

}
