package com.flipkart.varadhi.entities.filters;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Multimap;
import jakarta.validation.constraints.NotNull;

public class StringConditions {

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


    public record StartsWithCondition(String key, String value) implements Condition {
        @JsonCreator
        public StartsWithCondition(
            @JsonProperty (value = "key", required = true) @NotNull String key,
            @JsonProperty (value = "value", required = true) @NotNull String value
        ) {
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


    public record ExistsCondition(String key) implements Condition {
        @JsonCreator
        public ExistsCondition(@JsonProperty (value = "key", required = true) String key) {
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


    public record InCondition(String key, List<String> values) implements Condition {
        @JsonCreator
        public InCondition(
            @JsonProperty (value = "key", required = true) @NotNull String key,
            @JsonProperty (value = "values", required = true) @NotNull List<String> values
        ) {
            this.key = key;
            this.values = values;
        }

        @Override
        public boolean evaluate(Multimap<String, String> headers) {
            return headers.get(key).stream().anyMatch(values::contains);
        }

        @Override
        public String toString() {
            return String.format(
                "in(%s,[%s])",
                key,
                values.stream().map(value -> String.format("\"%s\"", value)).collect(Collectors.joining(","))
            );
        }
    }
}
