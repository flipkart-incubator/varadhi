package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.entities.cluster.ShardOperation;

import java.util.concurrent.CompletableFuture;

/**
 * Consumer → controller callbacks over the controller route ({@code send}, delivery-tracked).
 */
public interface ConsumerCallbackApi {

    CompletableFuture<Void> update(String subOpId, String shardOpId, ShardOperation.State state, String errorMsg);
}
