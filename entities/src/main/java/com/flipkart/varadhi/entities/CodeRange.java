package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * CodeRange for some kind of codes.
 * from and to are inclusive bounds.
 * Supports JSON serialization/deserialization for VaradhiSubscription and CallbackConfig.
 */
@Data
public class CodeRange {
    private final int from;
    private final int to;

    @JsonCreator
    public CodeRange(@JsonProperty("from") int from, @JsonProperty("to") int to) {
        this.from = from;
        this.to = to;
    }

    public boolean inRange(final int code) {
        return code >= from && code <= to;
    }
}
