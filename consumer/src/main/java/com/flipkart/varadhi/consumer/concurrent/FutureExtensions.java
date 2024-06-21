package com.flipkart.varadhi.consumer.concurrent;

import java.util.concurrent.CompletableFuture;

public class FutureExtensions {

    public static <T> void handleCompletion(CompletableFuture<T> future, CompletableFuture<T> promise) {
        future.whenComplete((r, t) -> {
            if (t == null) {
                promise.complete(r);
            } else {
                promise.completeExceptionally(t);
            }
        });
    }
}
