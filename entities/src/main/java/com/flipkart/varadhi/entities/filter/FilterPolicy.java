package com.flipkart.varadhi.entities.filter;

import lombok.Data;

@Data
public class FilterPolicy {

    /**
     * Expression struct that describes the various conditions and subexpressions joined by binary or unary operators.
     */
    private final FilterExpression filterExpression;
}
