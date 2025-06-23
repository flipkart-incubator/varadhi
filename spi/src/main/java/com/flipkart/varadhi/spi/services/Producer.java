package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface Producer extends Closeable {

    CompletableFuture<Offset> produceAsync(Message message);

    @Override
    default void close() throws IOException {
    }
}
