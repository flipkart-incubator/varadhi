package com.flipkart.varadhi.entities.filters.conditions.comparison;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.filters.conditions.Condition;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@SuperBuilder
public class NotCondition implements Condition {
    private Condition values;

    @JsonCreator
    public NotCondition(@JsonProperty (value = "values", required = true) Condition value) {
        this.values = value;
    }

    @Override
    public String toString() {
        return "not(" + values.toString() + ")";
    }

    @Override
    public boolean evaluate(Multimap<String, String> headers) {
        return !values.evaluate(headers);
    }
}
