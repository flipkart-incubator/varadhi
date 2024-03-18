package com.flipkart.varadhi.components;

public enum ComponentKind {
    Server("server"),
    Controller("controller"),
    All("all");

    private final String name;
    ComponentKind(String name) {
        this.name = name;
    }
}
