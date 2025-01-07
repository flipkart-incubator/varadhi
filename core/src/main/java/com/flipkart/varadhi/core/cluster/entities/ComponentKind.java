package com.flipkart.varadhi.core.cluster.entities;

public enum ComponentKind {
    Server("Server"),
    Controller("Controller"),
    Consumer("Consumer");

    private final String name;

    ComponentKind(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
