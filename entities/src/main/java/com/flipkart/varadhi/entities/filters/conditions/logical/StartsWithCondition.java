package com.flipkart.varadhi.entities.filters.conditions.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.conditions.Condition;
import com.google.common.collect.Multimap;
import jakarta.validation.constraints.NotNull;

public record StartsWithCondition(String key, String value) implements Condition {
    @JsonCreator
    public StartsWithCondition(@JsonProperty (value = "key", required = true) @NotNull String key, @JsonProperty (value = "value", required = true) @NotNull String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean evaluate(Multimap<String, String> headers) {
        return headers.get(key).stream().anyMatch(headerValue -> headerValue.startsWith(value));
    }

    @Override
    public String toString() {
        return String.format("startsWith(%s,\"%s\")", key, value);
    }
}
