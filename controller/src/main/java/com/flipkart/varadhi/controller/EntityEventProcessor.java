package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.entities.EntityEvent;

import java.util.concurrent.CompletableFuture;

public interface EntityEventProcessor {
    CompletableFuture<Void> process(EntityEvent event);
}
