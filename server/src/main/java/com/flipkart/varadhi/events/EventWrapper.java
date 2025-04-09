package com.flipkart.varadhi.events;

import com.flipkart.varadhi.common.events.EntityEvent;
import lombok.Getter;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Wrapper for entity events that tracks processing status across cluster nodes.
 * <p>
 * This class provides thread-safe tracking of which nodes have completed processing
 * an event and signals when all nodes have finished.
 */
public final class EventWrapper {
    /**
     * The entity event being processed.
     */
    @Getter
    final EntityEvent<?> event;

    /**
     * Set of node hostnames that need to process this event.
     */
    final Set<String> nodes;

    /**
     * Set of node hostnames that have completed processing this event.
     */
    final Set<String> completedNodes;

    /**
     * Future that completes when all nodes have processed the event.
     */
    final CompletableFuture<Void> completeFuture;

    /**
     * Lock for thread-safe operations and condition signaling.
     */
    final ReentrantLock lock;

    /**
     * Condition variable for signaling when an event is ready.
     */
    final Condition readyCondition;

    /**
     * Creates a new event wrapper for the given event and set of nodes.
     *
     * @param event          The entity event to be processed
     * @param nodes          The set of nodes that need to process this event
     * @param completeFuture The future to complete when all nodes have processed the event
     * @throws NullPointerException if any parameter is null
     */
    public EventWrapper(EntityEvent<?> event, Set<String> nodes, CompletableFuture<Void> completeFuture) {
        this.event = Objects.requireNonNull(event, "Event cannot be null");
        this.completeFuture = Objects.requireNonNull(completeFuture, "CompleteFuture cannot be null");

        // Create thread-safe sets for nodes
        this.nodes = ConcurrentHashMap.newKeySet();
        this.nodes.addAll(nodes);
        this.completedNodes = ConcurrentHashMap.newKeySet();

        // Initialize lock and condition
        this.lock = new ReentrantLock();
        this.readyCondition = lock.newCondition();
    }

    /**
     * Marks a node as having completed processing this event.
     * <p>
     * If all nodes have completed processing, signals any waiting threads.
     *
     * @param hostname The hostname of the node that completed processing
     */
    public synchronized void markNodeComplete(String hostname) {
        lock.lock();
        try {
            completedNodes.add(hostname);

            if (isCompleteForAllNodes()) {
                readyCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks if all nodes have completed processing this event.
     *
     * @return true if all nodes have completed processing, false otherwise
     */
    boolean isCompleteForAllNodes() {
        return !nodes.isEmpty() && completedNodes.containsAll(nodes);
    }

    /**
     * Returns the number of nodes that have completed processing this event.
     *
     * @return the count of nodes that have completed processing
     */
    public int getCompletedNodeCount() {
        return completedNodes.size();
    }

    /**
     * Returns the total number of nodes that need to process this event.
     *
     * @return the total count of nodes
     */
    public int getTotalNodeCount() {
        return nodes.size();
    }

    /**
     * Waits until either all nodes have completed processing or the specified timeout elapses.
     * <p>
     * This method must be called while not holding the lock, as it will acquire the lock internally.
     *
     * @param timeoutMs The maximum time to wait in milliseconds
     * @return true if the event is complete or the thread was signaled, false if the waiting time elapsed
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean awaitReady(long timeoutMs) throws InterruptedException {
        lock.lock();
        try {
            if (isCompleteForAllNodes()) {
                return true;
            }

            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            final long deadline = System.nanoTime() + remainingNanos;

            while (!isCompleteForAllNodes() && remainingNanos > 0) {
                long nanosLeft = readyCondition.awaitNanos(remainingNanos);

                if (isCompleteForAllNodes()) {
                    return true;
                }

                if (nanosLeft <= 0) {
                    return false;
                }

                remainingNanos = deadline - System.nanoTime();
            }

            return isCompleteForAllNodes();
        } finally {
            lock.unlock();
        }
    }
}
