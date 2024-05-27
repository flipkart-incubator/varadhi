package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.RequiredArgsConstructor;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Message source that does not maintain any kind of ordering.
 */
@RequiredArgsConstructor
public class UnGroupedMessageSrc<O extends Offset> implements MessageSrc {

    private final Consumer<O> consumer;

    // flag to indicate whether a future is in progress to fetch messages from the consumer.
    private final AtomicBoolean futureInProgress = new AtomicBoolean(false);

    // Iterator into an ongoing consumer batch that has not been fully processed yet.
    private Iterator<PolledMessage<O>> ongoingIterator = null;

    /**
     * Fetches the next batch of messages from the consumer.
     * Prioritise immediate fetch and return over waiting for the consumer.
     *
     * @param messages Array of message trackers to populate.
     *
     * @return CompletableFuture that completes when the messages are fetched.
     */
    @Override
    public CompletableFuture<Integer> nextMessages(MessageTracker[] messages) {
        // Iterator of consumer batch is cached when consumer batch size is more than messages size.
        // Our first priority is to drain the iterator if it is set and return immediately.
        // We do not want to proceed with consumer receiveAsync if we have messages in the iterator,
        // as a slow or empty consumer might block the flow and cause the iterator contents to be stuck.
        int offset = fetchFromIterator(ongoingIterator, messages, 0);
        if (offset > 0) {
            return CompletableFuture.completedFuture(offset);
        }

        // If the iterator is not set, or is empty, then we try to fetch the message batch from the consumer.
        // However, multiple calls to nextMessages may fire multiple futures concurrently.
        // Leading to a race condition that overrides the iterator from a previous un-processed batch, causing a lost-update problem.
        // Therefore, we use the futureInProgress flag to limit the concurrency and ensure only one future is in progress at a time.
        if (futureInProgress.compareAndSet(false, true)) {
            return consumer.receiveAsync()
                    .thenApply(polledMessages -> processPolledMessages(polledMessages, messages, offset))
                    .whenComplete((result, ex) -> futureInProgress.set(
                            false)); // any of the above stages can complete exceptionally, so this is to ensure the flag is reset.
        }
        return CompletableFuture.completedFuture(0);
    }

    private int processPolledMessages(PolledMessages<O> polledMessages, MessageTracker[] messages, int startIndex) {
        ongoingIterator = polledMessages.iterator();
        return fetchFromIterator(ongoingIterator, messages, startIndex);
    }

    /**
     * Fetches messages from the iterator and populates the message array.
     *
     * @param iterator   Iterator of messages to fetch from.
     * @param messages   Array of message trackers to populate.
     * @param startIndex Index into the messages array from where to start storing the messages.
     *
     * @return Index into the messages array where the next message should be stored. (will be equal to the length if completely full)
     */
    private int fetchFromIterator(
            Iterator<PolledMessage<O>> iterator, MessageTracker[] messages, int startIndex
    ) {
        while (iterator != null && iterator.hasNext() && startIndex < messages.length) {
            messages[startIndex++] = new PolledMessageTracker<>(consumer, iterator.next());
        }
        return startIndex;
    }
}
