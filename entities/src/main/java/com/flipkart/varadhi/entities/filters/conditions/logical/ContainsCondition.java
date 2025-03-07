package com.flipkart.varadhi.entities.filters.conditions.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.conditions.Condition;
import com.google.common.collect.Multimap;

public record ContainsCondition(String key, String value) implements Condition {
    @JsonCreator
    public ContainsCondition(
        @JsonProperty (value = "key", required = true) String key,
        @JsonProperty (value = "value", required = true) String value
    ) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean evaluate(Multimap<String, String> headers) {
        return headers.get(key).stream().anyMatch(headerValue -> headerValue.contains(value));
    }

    @Override
    public String toString() {
        return String.format("contains(%s,\"%s\")", key, value);
    }
}
