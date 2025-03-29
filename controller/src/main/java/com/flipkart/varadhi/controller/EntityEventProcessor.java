package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.common.events.EntityEvent;

import java.util.concurrent.CompletableFuture;

public interface EntityEventProcessor {
    <T> CompletableFuture<Void> process(EntityEvent<T> event);
}
