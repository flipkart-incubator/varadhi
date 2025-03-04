package com.flipkart.varadhi.entities.filters.conditions.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.conditions.Condition;
import com.google.common.collect.Multimap;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public record InCondition(String key, List<String> values) implements Condition {
    @JsonCreator
    public InCondition(@JsonProperty (value = "key", required = true) @NotNull String key, @JsonProperty (value = "values", required = true) @NotNull List<String> values) {
        this.key = key;
        this.values = values;
    }

    @Override
    public boolean evaluate(Multimap<String, String> headers) {
        return headers.get(key).stream().anyMatch(values::contains);
    }

    @Override
    public String toString() {
        return String.format("in(%s,[%s])", key, values.stream()
                .map(value -> String.format("\"%s\"", value))
                .collect(Collectors.joining(",")));
    }
}
