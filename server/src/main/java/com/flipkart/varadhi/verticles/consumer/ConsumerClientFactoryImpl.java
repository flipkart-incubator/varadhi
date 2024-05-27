package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;

public class ConsumerClientFactoryImpl implements ConsumerClientFactory {
    private final MessageExchange messageExchange;

    public ConsumerClientFactoryImpl(MessageExchange messageExchange) {
        this.messageExchange = messageExchange;
    }

    @Override
    public ConsumerApi getInstance(String consumerId) {
        return new ConsumerClient(consumerId, messageExchange);

    }
}
