package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.cluster.ConsumerState;

/**
 * Base varadhi consumer to consume messages from a varadhi topic. It corresponds to a single shard of some
 * subscription.
 */
public interface VaradhiConsumer {

    String getSubscriptionName();

    int getShardId();

    ConsumerState getState();

    /**
     * Initializes the consumer.
     *
     * @throws Exception bad configuration can result in exception in which case the consumer will not start.
     */
    void connect();

    /**
     * It will start message delivery from the last committed offset. If no offset is committed, it
     * can start from the earliest or latest offset based on the implementation.
     */
    void start();

    /**
     * Pause the consumer. It just means that message delivery will stop happening immediately.
     */
    void pause();

    /**
     * Resume from paused state. It means that message delivery will start happening immediately.
     */
    void resume();

    /**
     * Close the consumer. It will stop message delivery and release all resources. After calling this calling other
     * methods on this consumer will result in an exception.
     */
    void close();
}
