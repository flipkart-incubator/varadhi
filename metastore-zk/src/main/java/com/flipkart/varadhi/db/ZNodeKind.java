package com.flipkart.varadhi.db;

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
public record ZNodeKind(String kind, String pathFormat, boolean relative) {

    public ZNodeKind(String kind, String pathFormat) {
        this(kind, pathFormat, true);
    }

    /**
     * Creates a new ZNodeKind instance with validation.
     *
     * @throws NullPointerException if kind is null
     * @throws IllegalArgumentException if kind is blank
     */
    public ZNodeKind {
        if (kind == null || kind.isBlank()) {
            throw new NullPointerException("kind cannot be null / blank");
        }
        if (pathFormat == null || pathFormat.isBlank()) {
            throw new IllegalArgumentException("path format cannot be null / blank");
        }
    }

    public String resolvePath(String namespace, Object... args) {
        if (relative) {
            return (namespace + "/" + kind + "/" + pathFormat).formatted(args);
        } else {
            return (namespace + "/" + pathFormat).formatted(args);
        }
    }
}
