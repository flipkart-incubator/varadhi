package com.flipkart.varadhi.entities;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class VaradhiSubscription extends MetaStoreEntity {
    String project;
    String topic;
    String description;
    boolean grouped;
    Endpoint endpoint;

    public VaradhiSubscription(
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
