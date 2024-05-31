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

    /**
     * flag to indicate whether a task to fetch messages from consumer is ongoing.
     */
    private final AtomicBoolean pendingAsyncFetch = new AtomicBoolean(false);

    /**
     * Iterator into an ongoing consumer batch that has not been fully processed yet.
     */
    private volatile Iterator<PolledMessage<O>> ongoingIterator = null;

    /**
     * Fetches the next batch of messages from the consumer.
     * Prioritises returning whatever messages are available.
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
        int count = fetchFromIterator(consumer, messages, ongoingIterator);
        if (count > 0) {
            return CompletableFuture.completedFuture(count);
        }

        // If the iterator is not set, or is empty, then we try to fetch the message batch from the consumer.
        // However, multiple calls to nextMessages may fire multiple futures concurrently.
        // Leading to a race condition that overrides the iterator from a previous un-processed batch, causing a lost-update problem.
        // Therefore, we use the futureInProgress flag to limit the concurrency and ensure only one future is in progress at a time.
        ongoingIterator = null;
        if (pendingAsyncFetch.compareAndSet(false, true)) {
            return consumer.receiveAsync().whenComplete((result, ex) -> pendingAsyncFetch.set(false))
                    .thenApply(polledMessages -> processPolledMessages(polledMessages, messages));
        } else {
            throw new IllegalStateException(
                    "nextMessages method is not supposed to be called concurrently. There seems to be an ongoing consumer.receiveAsync() operation.");
        }
    }

    private int processPolledMessages(PolledMessages<O> polledMessages, MessageTracker[] messages) {
        Iterator<PolledMessage<O>> polledMessagesIterator = polledMessages.iterator();
        ongoingIterator = polledMessagesIterator;
        return fetchFromIterator(consumer, messages, polledMessagesIterator);
    }

    /**
     * Fetches messages from the iterator and populates the message array.
     *
     * @param iterator Iterator of messages to fetch from.
     * @param messages Array of message trackers to populate.
     *
     * @return Index into the messages array where the next message should be stored. (will be equal to the length if completely full)
     */

    static <O extends Offset> int fetchFromIterator(
            Consumer<O> consumer, MessageTracker[] messages, Iterator<PolledMessage<O>> iterator
    ) {
        if (iterator == null || !iterator.hasNext()) {
            return 0;
        }

        int i = 0;
        while (i < messages.length && iterator.hasNext()) {
            PolledMessage<O> polledMessage = iterator.next();
            MessageTracker messageTracker = new PolledMessageTracker<>(consumer, polledMessage);
            messages[i++] = messageTracker;
        }
        return i;
    }
}
