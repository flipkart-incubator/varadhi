package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.common.FutureExtensions;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A specialized consumer for RQs. It applies constant delay on all messages based on their produce time.
 * Allows changing the delay in runtime.
 *
 * TODO: allow passing a filter that recognizes some messages as follow-through and not apply delay on them.
 * TODO: limit the msg count returned.
 *
 * Not thread safe. All interaction to this class should be done on the context.
 *
 * @param <O>
 */
@Slf4j
@ExtensionMethod ({FutureExtensions.class})
public class DelayedConsumer<O extends Offset> implements Consumer<O> {

    private final Consumer<O> delegate;
    private final Context context;
    @Getter
    private long delayMs;

    private final Batch<O> polledMessages = Batch.emptyBatch();

    public DelayedConsumer(Consumer<O> delegate, Context context, long delayMs) {
        this.delegate = delegate;
        this.context = context;
        this.delayMs = delayMs;
    }

    public void setDelayMsAsync(long delayMs) {
        context.runOnContext(() -> {
            this.delayMs = delayMs;
        });
    }

    /**
     * Repeated calls to this method is not expected.
     * Call this method only when previous future is completed.
     *
     * TODO: add guard and assertion.
     * Must be called on the context. The returned future will be completed on the context as well.
     *
     * @return
     */
    @Override
    public CompletableFuture<PolledMessages<O>> receiveAsync() {
        assert context.isInContext();
        CompletableFuture<PolledMessages<O>> promise = new CompletableFuture<>();
        log.debug("called receive. promise: {}", promise);

        if (polledMessages.isEmpty()) {
            delegate.receiveAsync().whenComplete((messages, t) -> {
                if (t != null) {
                    promise.completeExceptionally(t);
                } else {
                    context.runOnContext(() -> {
                        polledMessages.add(messages);
                        findConsumableMsgs(promise);
                    });
                }
            });
            return promise;
        } else {
            findConsumableMsgs(promise);
        }
        return promise.whenComplete((p, t) -> log.debug("finishing receive. promise: {}", promise));
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
    private void findConsumableMsgs(CompletableFuture<PolledMessages<O>> promise) {
        assert context.isInContext();

        long now = System.currentTimeMillis();
        long earliestMessageTs = polledMessages.earliestMessageTimestamp();
        if (earliestMessageTs == Long.MAX_VALUE) {
            throw new IllegalStateException("this is not expected");
        }

        long timeLeft = Math.max(0, delayMs - (now - earliestMessageTs));

        if (timeLeft == 0) {
            promise.complete(polledMessages.getConsumableMsgs(delayMs));
        } else {
            log.debug("dont have messages to consume just yet. delay : {}ms", timeLeft);
            context.scheduleOnContext(
                () -> promise.complete(polledMessages.getConsumableMsgs(delayMs)),
                timeLeft,
                TimeUnit.MILLISECONDS
            );
        }
    }

    static final class DelayedMessages<O extends Offset> {
        int offset = 0;
        final ArrayList<PolledMessage<O>> messages = new ArrayList<>();

        long earliestMessageTimestamp() {
            if (offset < messages.size()) {
                return messages.get(offset).getProducedTimestampMs();
            } else {
                return Long.MAX_VALUE;
            }
        }

        void clear() {
            offset = 0;
            messages.clear();
        }

        int drainConsumableMsgsTo(ArrayList<PolledMessage<O>> msgs, long cutoffMs) {
            int prevOffset = offset;
            while (offset < messages.size() && messages.get(offset).getProducedTimestampMs() <= cutoffMs) {
                msgs.add(messages.get(offset));
                ++offset;
            }
            return offset - prevOffset;
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

        static <O extends Offset> Batch<O> emptyBatch() {
            return new Batch<>(0, 0, new HashMap<>());
        }

        void add(PolledMessages<O> newMsgs) {
            if (isEmpty()) {
                // reset the fields. nullify the arrays.
                clear();
            }
            for (PolledMessage<O> message : newMsgs) {
                delayedMsgs.compute(message.getPartition(), (k, v) -> {
                    if (v == null) {
                        v = new DelayedMessages<>();
                    }
                    v.messages.add(message);
                    return v;
                });
            }
            total += newMsgs.getCount();
        }

        void clear() {
            total = 0;
            consumed = 0;
            delayedMsgs.values().forEach(DelayedMessages::clear);
        }

        boolean isEmpty() {
            return consumed >= total;
        }

        long earliestMessageTimestamp() {
            return delayedMsgs.values()
                              .stream()
                              .mapToLong(DelayedMessages::earliestMessageTimestamp)
                              .min()
                              .orElse(Long.MAX_VALUE);
        }

        PolledMessages<O> getConsumableMsgs(long delayMillis) {
            ;
            long cutoff = System.currentTimeMillis() - delayMillis;
            ArrayList<PolledMessage<O>> msgs = new ArrayList<>();
            for (var m : delayedMsgs.values()) {
                consumed += m.drainConsumableMsgsTo(msgs, cutoff);
            }
            return new PolledMessages.ArrayBacked<>(msgs);
        }
    }
}
