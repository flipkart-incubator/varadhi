package com.flipkart.varadhi.common;

import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public class Extensions {

    public static class LockExtensions {
        public static <T> T lockAndCall(Lock lock, Callable<T> task) throws Exception {
            lock.lock();
            try {
                return task.call();
            } finally {
                lock.unlock();
            }
        }

        public static <T> T lockAndSupply(Lock lock, Supplier<T> task) {
            lock.lock();
            try {
                return task.get();
            } finally {
                lock.unlock();
            }
        }

        public static void lockAndRun(Lock lock, Runnable task) {
            lock.lock();
            try {
                task.run();
            } finally {
                lock.unlock();
            }
        }
    }


    public static class FutureExtensions {

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
}
