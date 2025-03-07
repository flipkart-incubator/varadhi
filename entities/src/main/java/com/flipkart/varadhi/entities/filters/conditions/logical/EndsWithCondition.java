package com.flipkart.varadhi.entities.filters.conditions.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.conditions.Condition;
import com.google.common.collect.Multimap;

public record EndsWithCondition(String key, String value) implements Condition {
    @JsonCreator
    public EndsWithCondition(
        @JsonProperty (value = "key", required = true) String key,
        @JsonProperty (value = "value", required = true) String value
    ) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean evaluate(Multimap<String, String> headers) {
        return headers.get(key).stream().anyMatch(headerValue -> headerValue.endsWith(value));
    }

    @Override
    public String toString() {
        return String.format("endsWith(%s,\"%s\")", key, value);
    }
}
