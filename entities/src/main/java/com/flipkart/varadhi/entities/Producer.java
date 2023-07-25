package com.flipkart.varadhi.entities;

import java.util.concurrent.CompletableFuture;

public interface Producer {
    //TODO:: is this better as service instead of entity ?
    CompletableFuture<ProducerResult> ProduceAsync(Message message);
}
