package com.flipkart.varadhi.components;

public enum ComponentKind {
    Server("Server"),
    Controller("Controller"),
    All("All");

    private final String name;
    ComponentKind(String name) {
        this.name = name;
    }
}
