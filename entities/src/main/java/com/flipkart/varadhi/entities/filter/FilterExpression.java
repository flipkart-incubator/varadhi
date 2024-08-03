package com.flipkart.varadhi.entities.filter;

import lombok.Data;

import java.util.List;

@Data
public class FilterExpression {
    private final FilterOperation.BooleanOps op;
    private final List<FilterExpression> exp;
}
