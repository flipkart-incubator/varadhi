package com.flipkart.varadhi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * This class is aimed at simplifying work with {@code CompletableFuture}.
 */
public class FutureUtil {

    /**
     * Return a future that represents the completion of the futures in the provided Collection.
     *
     * @param futures futures to wait for
     * @return a new CompletableFuture that is completed when all the given CompletableFutures complete
     */
    public static CompletableFuture<Void> waitForAll(Collection<? extends CompletableFuture<?>> futures) {
        if (futures == null || futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    public static <T> CompletableFuture<List<T>> waitForAll(Stream<CompletableFuture<List<T>>> futures) {
        return futures.reduce(CompletableFuture.completedFuture(new ArrayList<>()),
                (pre, curr) -> pre.thenCompose(preV -> curr.thenApply(currV -> {
                    preV.addAll(currV);
                    return preV;
                })));
    }

}
