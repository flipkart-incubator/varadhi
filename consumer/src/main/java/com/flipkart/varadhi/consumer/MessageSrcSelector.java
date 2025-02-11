package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.entities.InternalQueueType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
public class MessageSrcSelector {

    private final Context context;
    private final Holder[] messageSrcs;
    private final AtomicReference<CompletableFuture<PolledMessageTrackers>> pendingRequest = new AtomicReference<>();

    /**
     * Using LinkedHashMap to receive the order of the message sources.
     *
     * @param msgSrcs
     */
    public MessageSrcSelector(Context context, LinkedHashMap<InternalQueueType, MessageSrc> msgSrcs, int batchSize) {
        this.context = context;
        this.messageSrcs = new Holder[msgSrcs.size()];
        int i = 0;
        for (var entries : msgSrcs.entrySet()) {
            var holder = new Holder(
                context,
                entries.getKey(),
                entries.getValue(),
                new MessageTracker[batchSize],
                this::tryCompleteRequest
            );
            // simulate the first fetch on the context
            context.executeOnContext(() -> {
                holder.recycle();
                return null;
            }).join();
            messageSrcs[i] = holder;
            ++i;
        }
    }

    public int getBatchSize() {
        return messageSrcs[0].messages.length;
    }

    public CompletableFuture<PolledMessageTrackers> nextMessages() {
        assert context.isInContext();
        CompletableFuture<PolledMessageTrackers> promise = new CompletableFuture<>();
        if (!pendingRequest.compareAndSet(null, promise)) {
            promise.completeExceptionally(new IllegalStateException("Only one request is allowed at a time"));
            return promise;
        }

        for (Holder holder : messageSrcs) {
            if (holder.fetcher.get() == null) {
                // No fetcher means -> fetcher is not running -> previous fetcher must have returned with messages.
                promise = tryCompleteRequest(holder);
                if (promise != null) {
                    return promise;
                }
            }
            // else, fetcher is non-null, so we don't have any msgs from this source. thus ignore.
        }

        return promise;
    }

    private CompletableFuture<PolledMessageTrackers> tryCompleteRequest(Holder holder) {
        CompletableFuture<PolledMessageTrackers> promise = pendingRequest.getAndSet(null);
        if (promise != null) {
            log.debug(
                "returning messages from message src of type: {}. msgs now: {}",
                holder.internalQueueType,
                holder.size
            );
            promise.complete(new PolledMessageTrackers(holder));
            return promise;
        } else {
            log.debug(
                "fetched new message for the message src, no pending request to finish: {}",
                holder.internalQueueType
            );
            return null;
        }
    }

    @RequiredArgsConstructor
    private static final class Holder {
        private final Context context;
        private final InternalQueueType internalQueueType;
        private final MessageSrc msgSrc;
        private final MessageTracker[] messages;
        private int size = 0;
        private final AtomicReference<Future<Integer>> fetcher = new AtomicReference<>();
        private final Consumer<Holder> onFetchComplete;

        public void recycle() {
            assert context.isInContext();
            assert fetcher.get() == null;

            int currentSize = size;
            size = 0;
            Arrays.fill(messages, 0, currentSize, null);

            log.debug("IQ:[{}]. Recycling messages array. Fetching new messages", internalQueueType);
            var nextFetch = msgSrc.nextMessages(messages);
            fetcher.set(nextFetch);
            log.debug("IQ:[{}]. New messages future got created: {}", internalQueueType, fetcher.get());
            nextFetch.whenComplete((count, _ignored) -> {
                size = count;
                fetcher.set(null);
                log.debug("IQ:[{}]. New messages future got completed: {}", internalQueueType, fetcher.get());
                onFetchComplete.accept(this);
            });
        }
    }


    @RequiredArgsConstructor
    public static final class PolledMessageTrackers {
        private final Holder holder;

        public InternalQueueType getInternalQueueType() {
            return holder.internalQueueType;
        }

        public MessageTracker[] getMessages() {
            return holder.messages;
        }

        public int getSize() {
            return holder.size;
        }

        public void recycle() {
            holder.recycle();
        }
    }
}
