package com.flipkart.varadhi.consumer.impl;

import com.flipkart.varadhi.consumer.ConsumerState;
import com.flipkart.varadhi.consumer.ConsumersManager;
import com.flipkart.varadhi.consumer.ConsumptionFailurePolicy;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.spi.services.TopicPartitions;

import java.util.concurrent.CompletableFuture;

public class ConsumersManagerImpl implements ConsumersManager {


    public ConsumersManagerImpl() {
    }

    @Override
    public CompletableFuture<Void> startSubscription(
            Project project, String subscription, String shardName, TopicPartitions<StorageTopic> topic,
            boolean grouped, Endpoint endpoint, ConsumptionPolicy consumptionPolicy,
            ConsumptionFailurePolicy failurePolicy
    ) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopSubscription(String subscription, String shardName) {
        return null;
    }

    @Override
    public void pauseSubscription(String subscription, String shardName) {

    }

    @Override
    public void resumeSubscription(String subscription, String shardName) {

    }

    @Override
    public ConsumerState getConsumerState(String subscription, String shardName) {
        return null;
    }
}
