package com.flipkart.varadhi.common;

import io.vertx.core.Future;
import lombok.SneakyThrows;

public class TestExtensions {

    public static class FutureExtensions {

        @SneakyThrows
        public static <T> T blockingGet(Future<T> future) {
            return future.toCompletionStage().toCompletableFuture().get();
        }
    }
}
