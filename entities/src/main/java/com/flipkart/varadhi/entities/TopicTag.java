package com.flipkart.varadhi.entities;

/**
 * Represents tags that can be associated with a topic for classification and management.
 * <p>
 * Tags help in capacity planning, monitoring, and operational management of topics.
 */
public enum TopicTag {
    /**
     * Production environment tag.
     */
    PROD,

    /**
     * Non-production environment tag (e.g., staging, dev, test).
     */
    NON_PROD,

    /**
     * High priority topic tag - indicates topics that require special attention
     * during capacity planning and failover scenarios.
     */
    HIGH_PRIORITY
}
