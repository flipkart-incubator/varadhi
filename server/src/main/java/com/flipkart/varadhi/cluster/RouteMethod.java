package com.flipkart.varadhi.cluster;

public enum RouteMethod {

    SEND("send"), REQUEST("request"), PUBLISH("publish");

    private final String name;

    RouteMethod(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
