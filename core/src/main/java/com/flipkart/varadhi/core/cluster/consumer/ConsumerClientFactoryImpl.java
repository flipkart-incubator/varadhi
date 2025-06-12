package com.flipkart.varadhi.core.cluster.consumer;

import com.flipkart.varadhi.core.cluster.MessageExchange;

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
