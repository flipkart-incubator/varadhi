package com.flipkart.varadhi.db;

import java.util.Objects;

/**
 * Represents the type or kind of ZooKeeper node in the system.
 * <p>
 * This class is immutable and thread-safe, providing a type-safe way to
 * handle ZooKeeper node kinds across the application.
 * <p>
 * Usage example:
 * <pre>{@code
 * var nodeKind = new ZNodeKind("topic");
 * }</pre>
 */
public record ZNodeKind(String kind) {
    /**
     * Creates a new ZNodeKind instance with validation.
     *
     * @throws NullPointerException if kind is null
     * @throws IllegalArgumentException if kind is blank
     */
    public ZNodeKind {
        Objects.requireNonNull(kind, "kind cannot be null");
        if (kind.isBlank()) {
            throw new IllegalArgumentException("kind cannot be blank");
        }
    }
}
