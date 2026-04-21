package com.flipkart.varadhi.entities;

/**
 * Classifies standard headers for queue produce: required only on queue produce ({@link #Queue}), or for shared
 * usage including queue produce ({@link #All} / {@link #produce()}).
 * <p>
 * In YAML/JSON, serialize as {@code None}, {@code Queue}, or {@code All}. On {@link HeaderSpec}, a missing
 * {@code requiredBy} defaults to {@link #None}. {@link com.flipkart.varadhi.entities.StdHeaders#getHeaderNamesRequiredForQueueProduce()}
 * lists every header whose {@code requiredBy} is {@link #Queue} or {@link #All}.
 */
public enum RequiredBy {
    None, Queue,
    /**
     * Required for queue produce and for other message flows (e.g. message id). Prefer referring to this role in code
     * via {@link #produce()}; in config use the enum name {@code All}.
     */
    All;

    /**
     * Same as {@link #All}: header must be present for queue produce and for other flows (e.g. message id).
     * Use this in Java instead of raw {@code All} where the intent is “mandatory for those produce paths”.
     */
    public static RequiredBy produce() {
        return All;
    }

    /**
     * Whether this header spec applies to the queue produce API (headers that must be sent when producing to a queue).
     */
    public boolean isRequiredOnQueueProduce() {
        return this == Queue || this == All;
    }
}
