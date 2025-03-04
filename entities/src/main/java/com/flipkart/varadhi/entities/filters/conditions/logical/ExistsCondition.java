package com.flipkart.varadhi.entities.filters.conditions.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.conditions.Condition;
import com.google.common.collect.Multimap;

public record ExistsCondition(String key) implements Condition {
    @JsonCreator
    public ExistsCondition(@JsonProperty (value = "key", required = true)
    String key) {
        this.key = key;
    }

    @Override
    public boolean evaluate(Multimap<String, String> headers) {
        return headers.containsKey(key);
    }

    @Override
    public String toString() {
        return String.format("exists(%s)", key);
    }
}
