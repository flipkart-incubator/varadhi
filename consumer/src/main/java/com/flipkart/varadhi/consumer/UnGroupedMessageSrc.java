package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Message source that does not maintain any kind of ordering.
 */
@Slf4j
@RequiredArgsConstructor
public class UnGroupedMessageSrc<O extends Offset> implements MessageSrc {

    private final InternalQueueType queueType;

    private final Consumer<O> consumer;

    private final ConsumerMetrics metrics;

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
     * @param result Array of message trackers to populate.
     *
     * @return CompletableFuture that completes when the messages are fetched.
     */
    @Override
    public CompletableFuture<Integer> nextMessages(MessageTracker[] result) {
        // Iterator of consumer batch is cached when `consumer batch size` is more than `results` size.
        // Our first priority is to drain the iterator if it is set and return immediately.
        // We do not want to proceed with consumer receiveAsync if we have messages in the iterator,
        // as a slow or empty consumer might block the flow and cause the iterator contents to be stuck.
        // TODO: we might asynchronously fetch the next batch from the consumer if the iterator is almost empty.
        int count = fetchFromIterator(result, ongoingIterator);
        if (count > 0) {
            log.debug("IQ: [{}]. returning {} message from buffer", queueType, count);
            return CompletableFuture.completedFuture(count);
        }

        // If the iterator is not set, or is empty, then we try to fetch the message batch from the consumer.
        // However, multiple calls to nextMessages may fire multiple futures concurrently.
        // Leading to a race condition that overrides the iterator from a previous un-processed batch, causing a lost-update problem.
        // Therefore, we use the futureInProgress flag to limit the concurrency and ensure only one future is in progress at a time.
        ongoingIterator = null;
        if (pendingAsyncFetch.compareAndSet(false, true)) {
            log.debug("IQ: [{}]. fetching messages from consumer", queueType);
            return consumer.receiveAsync().thenApply(polledMessages -> {
                int processedCount = processPolledMessages(polledMessages, result);
                pendingAsyncFetch.set(false);
                return processedCount;
            });
        } else {
            throw new IllegalStateException(
                "nextMessages method is not supposed to be called concurrently. There seems to be an ongoing consumer.receiveAsync() operation."
            );
        }
    }

    private int processPolledMessages(PolledMessages<O> polledMessages, MessageTracker[] messages) {
        Iterator<PolledMessage<O>> polledMessagesIterator = polledMessages.iterator();
        int count = fetchFromIterator(messages, polledMessagesIterator);
        log.debug(
            "IQ: [{}]. received {} messages from consumer. returning {} msgs.",
            queueType,
            polledMessages.getCount(),
            count
        );
        if (polledMessagesIterator.hasNext()) {
            ongoingIterator = polledMessagesIterator;
        }
        return count;
    }

    /**
     * Fetches messages from the iterator and populates the message array.
     *
     * @param iterator Iterator of messages to fetch from.
     * @param messages Array of message trackers to populate.
     *
     * @return Index into the messages array where the next message should be stored. (will be equal to the length if
     *         completely full)
     */

    int fetchFromIterator(MessageTracker[] messages, Iterator<PolledMessage<O>> iterator) {
        if (iterator == null || !iterator.hasNext()) {
            return 0;
        }

        int i = 0;
        while (i < messages.length && iterator.hasNext()) {
            PolledMessage<O> polledMessage = iterator.next();
            MessageTracker messageTracker = new PolledMessageTracker<>(consumer, polledMessage, metrics::begin);
            messages[i++] = messageTracker;
        }

        return i;
    }
}
