package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.cluster.messages.ShardMessage;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ShardOperation;

public class ConsumerApiProxy implements ConsumerApi {
    private final MessageExchange exchange;
    private final String consumerId;

    public ConsumerApiProxy(String consumerId, MessageExchange exchange) {
        this.consumerId = consumerId;
        this.exchange = exchange;
    }

    @Override
    public void start(ShardOperation.StartData operation) {
        ShardMessage shardMessage = new ShardMessage(operation);
        exchange.send(consumerId, "start", shardMessage);
    }
}
