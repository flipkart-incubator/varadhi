package com.flipkart.varadhi.entities.filter;

public enum FilterOperation {
    startsWith,
    endsWith,
    contains,
    in,
    exists;

    public enum BooleanOps {
        AND, OR, NOT;
    }
}
