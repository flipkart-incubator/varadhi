package com.flipkart.varadhi.consumer.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class FutureExtensions {

    public static <T> void handleCompletion(CompletionStage<T> future, CompletableFuture<T> promise) {
        future.whenComplete((r, t) -> {
            if (t == null) {
                promise.complete(r);
            } else {
                promise.completeExceptionally(t);
            }
        });
    }
}
