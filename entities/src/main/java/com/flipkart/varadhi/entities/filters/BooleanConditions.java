package com.flipkart.varadhi.entities.filters;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Multimap;

/**
 * Provides boolean logic implementations of the Condition interface.
 * These classes can be used to create complex boolean expressions for filtering.
 */
public class BooleanConditions {

    public record AndCondition(List<Condition> values) implements Condition {

        @JsonCreator
        public AndCondition(@JsonProperty (value = "values", required = true) List<Condition> values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return values.stream().map(Condition::toString).collect(Collectors.joining(" and "));
        }

        @Override
        public boolean evaluate(Multimap<String, String> headers) {
            return values.stream().allMatch(condition -> condition.evaluate(headers));
        }
    }


    public record NandCondition(List<Condition> values) implements Condition {

        @JsonCreator
        public NandCondition(@JsonProperty (value = "values", required = true) List<Condition> values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return "not(" + values.stream().map(Condition::toString).collect(Collectors.joining(" and ")) + ")";
        }

        @Override
        public boolean evaluate(Multimap<String, String> headers) {
            return !values.stream().allMatch(condition -> condition.evaluate(headers));
        }
    }


    public record OrCondition(List<Condition> values) implements Condition {

        @JsonCreator
        public OrCondition(@JsonProperty (value = "values", required = true) List<Condition> values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return values.stream().map(Condition::toString).collect(Collectors.joining(" or "));
        }

        @Override
        public boolean evaluate(Multimap<String, String> headers) {
            return values.stream().anyMatch(condition -> condition.evaluate(headers));
        }
    }


    public record NorCondition(List<Condition> values) implements Condition {

        @JsonCreator
        public NorCondition(@JsonProperty (value = "values", required = true) List<Condition> values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return "not(" + values.stream().map(Condition::toString).collect(Collectors.joining(" or ")) + ")";
        }

        @Override
        public boolean evaluate(Multimap<String, String> headers) {
            return values.stream().noneMatch(condition -> condition.evaluate(headers));
        }
    }


    public record NotCondition(Condition value) implements Condition {

        @JsonCreator
        public NotCondition(@JsonProperty (value = "value", required = true) Condition value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "not(" + value.toString() + ")";
        }

        @Override
        public boolean evaluate(Multimap<String, String> headers) {
            return !value.evaluate(headers);
        }
    }
}
