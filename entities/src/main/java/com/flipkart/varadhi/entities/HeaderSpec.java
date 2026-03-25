package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Spec for a standard header: header name value and where it is required (Subscription / Queue / Both / Callback).
 */
@JsonDeserialize (using = HeaderSpecDeserializer.class)
public record HeaderSpec(String value, RequiredBy requiredBy) {

    /**
     * For backward compatibility: when config has a plain string (e.g. msgId: "X_MESSAGE_ID"),
     * deserialize as value with requiredBy = Both.
     */
    public static HeaderSpec fromString(String value) {
        return new HeaderSpec(value, RequiredBy.Both);
    }
}
