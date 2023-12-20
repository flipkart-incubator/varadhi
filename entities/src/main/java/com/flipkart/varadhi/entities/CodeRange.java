package com.flipkart.varadhi.entities;

import lombok.Data;

/**
 * CodeRange for some kind of codes.
 * from and to are inclusive bounds
 */
@Data
public class CodeRange {
    private final int from;
    private final int to;

    public boolean inRange(final int code) {
        return code >= from && code <= to;
    }
}
