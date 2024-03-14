package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;

import java.util.concurrent.CompletableFuture;

/**
 * Consumer interface to receive messages from a topic.
 */
public interface Consumer<O extends Offset> extends AutoCloseable {

    /**
     * Receive a batch of messages from the subscribed topics.
     */
    CompletableFuture<PolledMessages<O>> receiveAsync();

    /**
     * Commit upto the offset, signifying that all messages upto (& including) the offset have been processed.
     */
    CompletableFuture<Void> commitCumulativeAsync(PolledMessage<O> message);
}
