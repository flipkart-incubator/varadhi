package com.flipkart.varadhi.entities.filters.conditions.comparison;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.conditions.Condition;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@SuperBuilder
public class AndCondition implements Condition {
    private List<Condition> values;

    @JsonCreator
    public AndCondition(@JsonProperty (value = "values", required = true)
    List<Condition> values) {
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
