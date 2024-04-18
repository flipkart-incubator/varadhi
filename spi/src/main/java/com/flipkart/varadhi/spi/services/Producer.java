package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;

import java.util.concurrent.CompletableFuture;

public interface Producer extends AutoCloseable {
    CompletableFuture<Offset> produceAsync(Message message);

    @Override
    default void close() throws Exception {
    }
}
