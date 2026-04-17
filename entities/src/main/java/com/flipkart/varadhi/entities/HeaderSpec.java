package com.flipkart.varadhi.entities;

/**
 * Spec for a standard header: header name value and queue-produce classification ({@link MandatoryBy}).
 * <p>
 * is omitted, it defaults to {@link MandatoryBy#Both} (same intent as {@link MandatoryBy#mandatoryHeaderRequiredForProduce()}
 * in Java).
 */
public record HeaderSpec(String value, MandatoryBy mandatoryBy) {

    public HeaderSpec {
        if (mandatoryBy == null) {
            mandatoryBy = MandatoryBy.None;
        }
    }

    /**
     * For callers that only supply the header name; uses {@link MandatoryBy#Both}.
     */
    public static HeaderSpec fromString(String value) {
        return new HeaderSpec(value, MandatoryBy.None);
    }
}
