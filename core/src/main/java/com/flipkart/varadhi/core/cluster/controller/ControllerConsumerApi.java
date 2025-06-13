package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.ShardOperation;

import java.util.concurrent.CompletableFuture;

public interface ControllerConsumerApi {
    String ROUTE_CONTROLLER = "controller";

    CompletableFuture<Void> update(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg);

}
