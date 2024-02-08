package com.flipkart.varadhi.services;

import com.flipkart.varadhi.consumer.ConsumerStatus;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import io.vertx.core.Future;

/**
 * Management layer that will manage consumers for multiple subscriptions.
 */
public interface ConsumersManager {

    /**
     * Start consumption for a shard of a subscription. If subscription failed, the future will contain the exception
     * details.
     *
     * @param subscription
     *
     * @return
     */
    Future<Void> startSubscription(VaradhiSubscription subscription);

    Future<Void> stopSubscription(String subscription, int shardId);

    Future<Void> pauseSubscription(String subscription, int shardId);

    Future<Void> resumeSubscription(String subscription, int shardId);

    ConsumerStatus getConsumerStatus(String subscription, int shardId);

    // TODO likely need status on the starting / stopping as well; as the above status is for a running consumer..
}
