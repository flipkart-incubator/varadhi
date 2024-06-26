package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.entities.cluster.Operation;

import java.util.concurrent.CompletableFuture;

public interface OpExecutor<T extends Operation> {
    CompletableFuture<Void> execute(T operation);

}
