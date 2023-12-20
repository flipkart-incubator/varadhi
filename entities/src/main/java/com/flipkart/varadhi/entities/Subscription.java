package com.flipkart.varadhi.entities;

import jakarta.validation.constraints.NotNull;

public class Subscription extends VaradhiResource {
    @NotNull
    private String topic;
    private boolean grouped;

    @NotNull
    private Endpoint endpoint;

    // TODO: add other policies here

    public Subscription(
            String name, int version, String topic, boolean grouped, Endpoint endpoint
    ) {
        super(name, version);
        this.topic = topic;
        this.grouped = grouped;
        this.endpoint = endpoint;
    }
}
