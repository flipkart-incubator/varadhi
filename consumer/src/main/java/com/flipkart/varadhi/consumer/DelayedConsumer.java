package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.concurrent.FutureExtensions;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.ExtensionMethod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A specialized consumer for RQs. It applies constant delay on all messages based on their produce time.
 * Allows changing the delay in runtime.
 *
 * TODO: allow passing a filter that recognizes some messages as follow-through and not apply delay on them.
 *
 * @param <O>
 */
@RequiredArgsConstructor
@ExtensionMethod({FutureExtensions.class})
public class DelayedConsumer<O extends Offset> implements Consumer<O> {

    private final Batch<O> polledMessages = Batch.emtpyBatch();
    private final Consumer<O> delegate;
    private final Context context;
    private final ScheduledExecutorService scheduler;

    @Getter
    private long delayMs;

    public void setDelayMsAsync(long delayMs) {
        context.runOnContext(() -> {
            this.delayMs = delayMs;
        });
    }

    /**
     * only to be called from the context
     *
     * @return
     */
    @Override
    public CompletableFuture<PolledMessages<O>> receiveAsync() {
        assert context.isInContext();

        if (polledMessages.isEmpty()) {
            CompletableFuture<PolledMessages<O>> promise = new CompletableFuture<>();
            delegate.receiveAsync().whenComplete((messages, t) -> {
                if (t != null) {
                    promise.completeExceptionally(t);
                } else {
                    context.runOnContext(() -> {
                        polledMessages.add(messages);
                        getConsumableMsgs().handleCompletion(promise);
                    });
                }
            });
            return promise;
        } else {
            return getConsumableMsgs();
        }
    }

    @Override
    public CompletableFuture<Void> commitIndividualAsync(PolledMessage<O> message) {
        return delegate.commitIndividualAsync(message);
    }

    @Override
    public CompletableFuture<Void> commitCumulativeAsync(PolledMessage<O> message) {
        return delegate.commitCumulativeAsync(message);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * to be run on context
     *
     * @return
     */
    private CompletableFuture<PolledMessages<O>> getConsumableMsgs() {
        assert context.isInContext();

        long now = System.currentTimeMillis();
        long earliestMessageTs = polledMessages.earliestMessageTimestamp();
        if (earliestMessageTs == Long.MAX_VALUE) {
            throw new IllegalStateException("this is not expected");
        }

        long timeLeft = Math.max(0, delayMs - (now - earliestMessageTs));

        CompletableFuture<PolledMessages<O>> delayedMsgs = new CompletableFuture<>();
        if (timeLeft == 0) {
            delayedMsgs.complete(polledMessages.getConsumableMsgs(delayMs));
        } else {
            scheduler.schedule(() -> {
                context.runOnContext(() -> delayedMsgs.complete(polledMessages.getConsumableMsgs(delayMs)));
            }, timeLeft, TimeUnit.MILLISECONDS);
        }
        return delayedMsgs;
    }

    @AllArgsConstructor
    static final class DelayedMessages<O extends Offset> {
        int arrOffset;
        final ArrayList<PolledMessage<O>> messages;

        long earliestMessageTimestamp() {
            if (arrOffset < messages.size()) {
                return messages.get(arrOffset).getProducedTimestampMs();
            } else {
                return Long.MAX_VALUE;
            }
        }
    }

    /**
     * Batch of messages per partition.
     */
    @AllArgsConstructor
    static class Batch<O extends Offset> {
        private int total;
        private int consumed;
        private final Map<Integer, DelayedMessages<O>> delayedMsgs;

        static <O extends Offset> Batch<O> emtpyBatch() {
            return new Batch<>(0, 0, new HashMap<>());
        }

        void add(PolledMessages<O> newMsgs) {
            if (isEmpty()) {
                // reset the fields. nullify the arrays.
                clear();
            }
            for (PolledMessage<O> message : newMsgs) {
                delayedMsgs.computeIfAbsent(
                                message.getPartition(), partition -> new DelayedMessages<>(0, new ArrayList<>())
                        )
                        .messages.add(message);
            }
            total += newMsgs.getCount();
        }

        void clear() {
            total = 0;
            consumed = 0;
            delayedMsgs.values().forEach(a -> a.messages.clear());
        }

        boolean isEmpty() {
            return consumed >= total;
        }

        long earliestMessageTimestamp() {
            return delayedMsgs.values().stream()
                    .mapToLong(DelayedMessages::earliestMessageTimestamp)
                    .min()
                    .orElse(Long.MAX_VALUE);
        }

        PolledMessages<O> getConsumableMsgs(long delayMillis) {
            long now = System.currentTimeMillis();
            ArrayList<PolledMessage<O>> msgs = new ArrayList<>();
            for (var m : delayedMsgs.values()) {
                while (m.arrOffset < m.messages.size() &&
                        (m.messages.get(m.arrOffset).getProducedTimestampMs() + delayMillis) <= now) {
                    msgs.add(m.messages.get(m.arrOffset));
                    ++m.arrOffset;
                }
            }
            return new PolledMessages.ArrayBacked<>(msgs);
        }
    }
}
