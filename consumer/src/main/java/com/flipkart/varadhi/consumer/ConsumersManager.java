package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.spi.services.TopicPartitions;
import io.vertx.core.Future;

/**
 * Management layer that will manage consumers for multiple subscriptions.
 */
public interface ConsumersManager {

    /**
     * Start consumption for a shard of a subscription. If subscription failed, the future will contain the exception
     * details.
     * `shardName` identifies the different shards within the subscription.
     *
     * @return
     */
    Future<Void> startSubscription(
            Project project,
            String subscription,
            String shardName,
            TopicPartitions<?> topic,
            boolean grouped,
            Endpoint endpoint,
            ConsumptionPolicy consumptionPolicy,
            ConsumptionFailurePolicy failurePolicy
    );

    Future<Void> stopSubscription(String subscription, String shardName);

    Future<Void> pauseSubscription(String subscription, String shardName);

    Future<Void> resumeSubscription(String subscription, String shardName);

    ConsumerState getConsumerState(String subscription, String shardName);

    // TODO likely need status on the starting / stopping as well; as the above status is for a running consumer..
}
