package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import com.google.common.collect.ArrayListMultimap;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

@RequiredArgsConstructor
public class UnGroupedMessageSrc<O extends Offset> implements MessageSrc {

    final Consumer<O> consumer;

    private final BlockingQueue<MessageTracker> buffer = new LinkedBlockingQueue<>();

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
            MessageTracker messageTracker = new UnGroupedMessageTracker<>(consumer, polledMessage);
            if (i < messages.length) {
                messages[i++] = messageTracker;
            } else {
                buffer.add(messageTracker);
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

    // TODO(aayush): dummy class for poc
    static class PolledMessageWrapper<O extends Offset> extends Message {
        PolledMessage<O> polledMessage;

        public PolledMessageWrapper(PolledMessage<O> polledMessage) {
            super(polledMessage.getPayload(), ArrayListMultimap.create());
            this.polledMessage = polledMessage;
        }
    }
}
