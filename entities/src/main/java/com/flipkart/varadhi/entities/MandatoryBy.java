package com.flipkart.varadhi.entities;

/**
 * Classifies standard headers for queue produce: required only on queue produce ({@link #Queue}), or for shared
 * usage including queue produce ({@link #Both} / {@link #mandatoryHeaderRequiredForProduce()}).
 * <p>
 * In YAML/JSON, serialize as {@code None}, {@code Queue}, or {@code Both}. On {@link HeaderSpec}, a missing
 * {@code mandatoryBy} defaults to {@link #None}. {@link com.flipkart.varadhi.entities.StdHeaders#getHeaderNamesRequiredForQueueProduce()}
 * lists every header whose {@code mandatoryBy} is {@link #Queue} or {@link #Both}.
 */
public enum MandatoryBy {
    None, Queue,
    /**
     * Required for queue produce and for other message flows (e.g. message id). Prefer referring to this role in code
     * via {@link #mandatoryHeaderRequiredForProduce()}; in config use the enum name {@code Both}.
     */
    Both;

    /**
     * Same as {@link #Both}: header must be present for queue produce and for other flows (e.g. message id).
     * Use this in Java instead of raw {@code Both} where the intent is “mandatory for those produce paths”.
     */
    public static MandatoryBy mandatoryHeaderRequiredForProduce() {
        return Both;
    }

    /**
     * Whether this header spec applies to the queue produce API (headers that must be sent when producing to a queue).
     */
    public boolean isRequiredOnQueueProduce() {
        return this == Queue || this == Both;
    }
}
