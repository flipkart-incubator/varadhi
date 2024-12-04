package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Consumer interface to receive messages from a topic.
 */
public interface Consumer<O extends Offset> extends Closeable {

    /**
     * Receive a batch of messages from the subscribed topics. Cancellable & never returns 0 messages.
     */
    CompletableFuture<PolledMessages<O>> receiveAsync();

    /**
     * Commit upto the offset, signifying that all messages upto (& including) the offset have been processed.
     */
    CompletableFuture<Void> commitCumulativeAsync(PolledMessage<O> message);

    /**
     * Commit the individual message, signifying that the message has been processed.\
     */
    CompletableFuture<Void> commitIndividualAsync(PolledMessage<O> message);
}
