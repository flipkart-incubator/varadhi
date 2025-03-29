package com.flipkart.varadhi.events;

import com.flipkart.varadhi.common.events.EntityEvent;
import lombok.Getter;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class EventWrapper {
    @Getter
    final EntityEvent<?> event;
    final Set<String> nodes;
    final Set<String> completedNodes;
    final CompletableFuture<Void> completeFuture;
    final AtomicBoolean nodesChanged;

    public EventWrapper(EntityEvent<?> event, Set<String> nodes, CompletableFuture<Void> completeFuture) {
        this.event = event;
        this.nodes = ConcurrentHashMap.newKeySet();
        this.nodes.addAll(nodes);
        this.completedNodes = ConcurrentHashMap.newKeySet();
        this.completeFuture = completeFuture;
        this.nodesChanged = new AtomicBoolean(false);
    }

    public synchronized void markNodeComplete(String hostname) {
        completedNodes.add(hostname);

        if (isCompleteForAllNodes()) {
            notifyAll();
        }
    }

    boolean isCompleteForAllNodes() {
        return !nodes.isEmpty() && completedNodes.containsAll(nodes);
    }

    public synchronized void markNodesChanged() {
        nodesChanged.set(true);
        notifyAll();
    }

    boolean hasNodesChanged() {
        return nodesChanged.get();
    }
}
