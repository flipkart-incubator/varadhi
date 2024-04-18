package com.flipkart.varadhi.consumer;

import java.util.concurrent.CompletableFuture;

public interface MessageSrc {

    /**
     * Fills the messages array with the next messages to be processed. The array size dictates the max number of
     * messages to return.
     *
     * @param messages
     * @return count of messages filled in the array.
     */
    CompletableFuture<Integer> nextMessages(MessageTracker[] messages);
}
