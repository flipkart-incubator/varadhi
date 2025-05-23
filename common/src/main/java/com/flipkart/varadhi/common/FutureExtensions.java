package com.flipkart.varadhi.common;

import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;

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

    public static <T> Future<T> record(Future<T> future, Timer.Sample clock, Timer timer) {
        return future.onComplete(result -> clock.stop(timer));
    }
}
