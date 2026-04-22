package com.flipkart.varadhi.entities;

/**
 * Classifies standard headers for queue produce: required only on queue produce ({@link #Queue}), or for shared
 * usage including queue produce ({@link #All}}).
 * <p>
 * In YAML/JSON, serialize as {@code None}, {@code Queue}, or {@code All}. On {@link HeaderSpec}, a missing
 * {@code requiredBy} defaults to {@link #None}. {@link com.flipkart.varadhi.entities.StdHeaders#getHeaderNamesRequiredForQueueProduce()}
 * lists every header whose {@code requiredBy} is {@link #Queue} or {@link #All}.
 */
public enum RequiredBy {
    None, Queue,
    /**
     * Required for queue produce and for other message flows i.e topic (e.g. message id)
     */
    All;

    /**
     * Whether this header spec applies to the queue produce API (headers that must be sent when producing to a queue).
     */
    public boolean isRequiredOnQueueProduce() {
        return this == Queue || this == All;
    }

    public boolean isRequiredOnProduce() {
        return this == All;
    }

}
