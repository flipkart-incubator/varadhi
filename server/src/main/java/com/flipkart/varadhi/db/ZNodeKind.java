package com.flipkart.varadhi.db;

import lombok.Getter;

@Getter
public class ZNodeKind {
    private final String kind;

    public ZNodeKind(String kind) {
        this.kind = kind;
    }
}
