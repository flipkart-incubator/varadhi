package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Message source that does not maintain any kind of ordering.
 */
@RequiredArgsConstructor
public class UnGroupedMessageSrc<O extends Offset> implements MessageSrc {

    private final Consumer<O> consumer;

    /**
     * Lock free buffer to store excess messages in case the consumer fetches a batch larger than the requested size.
     */
    private final ConcurrentLinkedQueue<MessageTracker> buffer = new ConcurrentLinkedQueue<>();

    /**
     * Fetches the next batch of messages from the consumer.
     * Attempts to first fetch from the buffer and then from the consumer.
     *
     * @param messages Array of message trackers to populate.
     *
     * @return CompletableFuture that completes when the messages are fetched.
     */
    @Override
    public CompletableFuture<Integer> nextMessages(MessageTracker[] messages) {
        int offset = fetchFromBuffer(messages);
        if (offset < messages.length) {
            return consumer.receiveAsync()
                    .thenApply(polledMessages -> processPolledMessages(offset, polledMessages, messages));
        }
        return CompletableFuture.completedFuture(offset);
    }

    private int processPolledMessages(int offset, PolledMessages<O> polledMessages, MessageTracker[] messages) {
        int i = offset;
        for (PolledMessage<O> polledMessage : polledMessages) {
            MessageTracker messageTracker = new PolledMessageTracker<>(consumer, polledMessage);
            if (i < messages.length) {
                messages[i++] = messageTracker;
            } else {
                buffer.offer(messageTracker);
            }
        }
        return i;
    }

    private int fetchFromBuffer(MessageTracker[] messages) {
        int i = 0;
        while (i < messages.length && !buffer.isEmpty()) {
            messages[i++] = buffer.poll();
        }
        return i;
    }
}
