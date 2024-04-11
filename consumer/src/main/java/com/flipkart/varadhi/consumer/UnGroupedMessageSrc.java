package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
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
        if (buffer.isEmpty()) {
            return consumer.receiveAsync().thenApply(polledMessages -> processPolledMessages(polledMessages, messages));
        } else {
            return CompletableFuture.completedFuture(processBuffer(messages));
        }
    }

    private int processPolledMessages(PolledMessages<O> polledMessages, MessageTracker[] messages) {
        int i = 0;
        for (PolledMessage<O> polledMessage : polledMessages) {
            MessageTracker messageTracker = new MessageTrackerImpl<>(polledMessage, consumer);
            if (i == messages.length) {
                buffer.add(messageTracker);
            } else {
                messages[i++] = messageTracker;
            }
        }
        return i;
    }

    private int processBuffer(MessageTracker[] messages) {
        int i = 0;
        while (i < messages.length && !buffer.isEmpty()) {
            messages[i++] = buffer.poll();
        }
        return i;
    }

    // dummy impl for poc
    static class MessageTrackerImpl<O extends Offset> implements MessageTracker {
        private final PolledMessage<O> message;
        private final Consumer<O> consumer;

        public MessageTrackerImpl(PolledMessage<O> message, Consumer<O> consumer) {
            this.message = message;
            this.consumer = consumer;
        }

        @Override
        public Message getMessage() {
            return new PolledMessageWrapper<>(message);
        }

        @Override
        public void onConsumed(MessageConsumptionStatus status) {
            if (status == MessageConsumptionStatus.SENT || status == MessageConsumptionStatus.FILTERED) {
                consumer.commitIndividualAsync(message).join(); // TODO: good?
            }
        }
    }

    // dummy class for poc
    static class PolledMessageWrapper<O extends Offset> extends Message {
        PolledMessage<O> polledMessage;

        public PolledMessageWrapper(PolledMessage<O> polledMessage) {
            super(polledMessage.getPayload(), null);
            this.polledMessage = polledMessage;
        }
    }
}
