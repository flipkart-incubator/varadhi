package com.flipkart.varadhi.verticles.webserver;

import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.ratelimit.SuppressionData;

public class SuppressorHandler<T> {
    SuppressionData<T> suppressionData;

    public SuppressorHandler() {
        this.suppressionData = new SuppressionData<>();
    }

    public T getSuppressionFactor(String topic) {
        return suppressionData.getSuppressionFactor().getOrDefault(topic, null);
    }

    public void handle(ClusterMessage message) {
        this.suppressionData = (SuppressionData<T>)message.getData(SuppressionData.class);
        suppressionData.getSuppressionFactor().forEach((topic, factor) -> {
//            this.suppressionData.getSuppressionFactor().put(topic, factor);
            System.out.printf("Got topic: %s, factor: %s%n", topic, factor);
        });
    }

}
