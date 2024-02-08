package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;

import java.util.concurrent.CompletableFuture;

/**
 * Consumer interface to receive messages from a topic.
 */
public interface Consumer<O extends Offset> {

    /**
     * Receive a batch of messages from the subscribed topics.
     *
     * @return
     */
    CompletableFuture<PolledMessages<O>> receiveAsync();

    /**
     * Commit upto the offset, signifying that all messages upto (& including) the offset have been processed.
     *
     * @param topic
     * @param partition
     * @param offset
     *
     * @return
     */
    CompletableFuture<Void> commitCummulative(String topic, int partition, O offset);

    /**
     * Unsubscribes the topics and closes the consumer object and any associated resources.
     */
    void close();

    // TODO: evaluate pause and resume methods.
}
