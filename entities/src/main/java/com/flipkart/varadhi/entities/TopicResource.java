package com.flipkart.varadhi.entities;

import lombok.Value;
import lombok.EqualsAndHashCode;

@Value
@EqualsAndHashCode(callSuper = true)
public class TopicResource extends VaradhiResource implements KeyProvider {

    private static final String RESOURCE_TYPE_NAME = "TopicResource";
    String project;
    boolean grouped;
    boolean exclusiveSubscription;
    CapacityPolicy capacityPolicy;

    //TODO::check if private constructor suffices.
    public TopicResource(String name,
                         int version,
                         String project,
                         boolean grouped,
                         boolean exclusiveSubscription,
                         CapacityPolicy capacityPolicy) {
        super(name, version);
        this.project = project;
        this.grouped = grouped;
        this.exclusiveSubscription = exclusiveSubscription;
        this.capacityPolicy = capacityPolicy;
    }
    @Override
    public String uniqueKeyPath() {
        return String.format("/%s/%s/%s", RESOURCE_TYPE_NAME, project, getName());
    }

}
