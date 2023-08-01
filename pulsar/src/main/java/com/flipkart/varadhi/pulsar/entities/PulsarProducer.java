package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProducerResult;
import com.flipkart.varadhi.spi.services.Producer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;


@Slf4j
public class PulsarProducer implements Producer {
    @Override
    public CompletableFuture<ProducerResult> ProduceAsync(Message message) {
        //TODO::Dummy code to let the flow work.
        CompletableFuture<ProducerResult> result = new CompletableFuture<ProducerResult>();
        result.complete(new PulsarProducerResult());
        return result;
    }
}
