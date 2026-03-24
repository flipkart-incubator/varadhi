package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Inclusive HTTP (or similar) code range: {@code from} and {@code to} are both inclusive.
 */
@Getter
@EqualsAndHashCode
@ToString
public class CodeRange {

    private final int from;
    private final int to;

    public CodeRange(int from, int to) {
        if (from > to) {
            throw new IllegalArgumentException(
                "CodeRange 'from' must be less than or equal to 'to' (inclusive bounds); got from=" + from + ", to="
                                               + to
            );
        }
        this.from = from;
        this.to = to;
    }

    public boolean inRange(final int code) {
        return code >= from && code <= to;
    }
}
