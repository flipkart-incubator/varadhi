package com.flipkart.varadhi.entities;

/**
 * Classifies standard headers for queue produce: required only on queue produce ({@link #Queue}), or for shared
 * usage including queue produce ({@link #Both} / {@link #mandatoryHeaderRequiredForProduce()}).
 * <p>
 * Reply-to and other non-queue-produce headers typically omit {@code requiredBy} in YAML (defaults per
 * {@link HeaderSpecDeserializer}); they are not included in
 * {@link com.flipkart.varadhi.entities.StdHeaders#getHeaderNamesRequiredForQueueProduce()}.
 */
public enum RequiredBy {
    Queue,
    /**
     * Required for queue produce and for other message flows (e.g. message id). Prefer referring to this role in code
     * via {@link #mandatoryHeaderRequiredForProduce()} and in YAML via {@code mandatoryHeaderRequiredForProduce}
     * (see {@link HeaderSpecDeserializer}).
     */
    Both;

    /**
     * Same as {@link #Both}: header must be present for queue produce and for other flows (e.g. message id).
     * Use this in Java instead of raw {@code Both} where the intent is “mandatory for those produce paths”.
     */
    public static RequiredBy mandatoryHeaderRequiredForProduce() {
        return Both;
    }

    /**
     * Whether this header spec applies to the queue produce API (headers that must be sent when producing to a queue).
     */
    public boolean isRequiredOnQueueProduce() {
        return this == Queue || this == Both;
    }
}
