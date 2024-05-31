package com.flipkart.varadhi.consumer.push;

import com.flipkart.varadhi.consumer.MessageTracker;
import lombok.AllArgsConstructor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A barebone standin for a message delivery client.
 */
public interface PushClient {

    // true == success, false == failed.
    CompletableFuture<PushResponse> push(MessageTracker messageTracker);

    @AllArgsConstructor
    class Flaky implements PushClient {

        private final double threshold;

        @Override
        public CompletableFuture<PushResponse> push(MessageTracker messageTracker) {
            return CompletableFuture.completedFuture(
                    new PushResponse(messageTracker, ThreadLocalRandom.current().nextDouble() > threshold));
        }
    }
}
