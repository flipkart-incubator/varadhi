package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Spec for a standard header: header name value and queue-produce classification ({@link RequiredBy}).
 */
@JsonDeserialize (using = HeaderSpecDeserializer.class)
public record HeaderSpec(String value, RequiredBy requiredBy) {

    /**
     * For backward compatibility: when config has a plain string (e.g. msgId: "X_MESSAGE_ID"),
     * deserialize with {@link RequiredBy#mandatoryHeaderRequiredForProduce()}.
     */
    public static HeaderSpec fromString(String value) {
        return new HeaderSpec(value, RequiredBy.mandatoryHeaderRequiredForProduce());
    }
}
