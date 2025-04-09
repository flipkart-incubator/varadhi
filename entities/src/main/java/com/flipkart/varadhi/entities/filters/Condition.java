package com.flipkart.varadhi.entities.filters;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Multimap;

/**
 * Represents a condition that can be evaluated against a set of headers.
 * Implementations of this interface should provide a string representation
 * of the condition and a method to evaluate the condition.
 */

@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "op")
@JsonSubTypes ({
    @JsonSubTypes.Type (value = BooleanConditions.AndCondition.class, name = "AND"),
    @JsonSubTypes.Type (value = BooleanConditions.OrCondition.class, name = "OR"),
    @JsonSubTypes.Type (value = BooleanConditions.NotCondition.class, name = "NOT"),
    @JsonSubTypes.Type (value = BooleanConditions.NandCondition.class, name = "NAND"),
    @JsonSubTypes.Type (value = BooleanConditions.NorCondition.class, name = "NOR"),
    @JsonSubTypes.Type (value = StringConditions.StartsWithCondition.class, name = "startsWith"),
    @JsonSubTypes.Type (value = StringConditions.EndsWithCondition.class, name = "endsWith"),
    @JsonSubTypes.Type (value = StringConditions.ContainsCondition.class, name = "contains"),
    @JsonSubTypes.Type (value = StringConditions.ExistsCondition.class, name = "exists"),
    @JsonSubTypes.Type (value = StringConditions.InCondition.class, name = "in")})
public interface Condition {

    /**
     * Returns a string representation of the condition.
     *
     * @return a string representation of the condition
     */
    String toString();

    /**
     * Evaluates the condition against the provided headers.
     *
     * @param headers a multimap of headers to evaluate the condition against
     * @return true if the condition is met, false otherwise
     */
    boolean evaluate(Multimap<String, String> headers);
}
