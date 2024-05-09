package com.flipkart.varadhi.verticles.consumer;

import com.flipkart.varadhi.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerApiFactory;

public class ConsumerApiFactoryImpl implements ConsumerApiFactory {
    private final MessageExchange messageExchange;

    public ConsumerApiFactoryImpl(MessageExchange messageExchange) {
        this.messageExchange = messageExchange;
    }
    @Override
    public ConsumerApi getConsumerProxy(String consumerId) {
        return new ConsumerApiProxy(consumerId, messageExchange);

    }
}
