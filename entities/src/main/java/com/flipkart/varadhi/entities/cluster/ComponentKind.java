package com.flipkart.varadhi.entities.cluster;

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
