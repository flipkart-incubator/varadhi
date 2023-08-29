package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProducerResult;

import java.util.concurrent.CompletableFuture;

public interface Producer {
    CompletableFuture<ProducerResult> ProduceAsync(Message message);
}
