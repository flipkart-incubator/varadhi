package com.flipkart.varadhi.entities;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class SubscriptionResource extends VersionedEntity {
    @NotBlank
    String project;

    @NotBlank
    String topic;

    @NotBlank
    String description;

    boolean grouped;

    @NotNull
    Endpoint endpoint;

    protected SubscriptionResource(
            String name,
            int version,
            String project,
            String topic,
            String description,
            boolean grouped,
            Endpoint endpoint
    ) {
        super(name, version);
        this.project = project;
        this.topic = topic;
        this.description = description;
        this.grouped = grouped;
        this.endpoint = endpoint;
    }
}
