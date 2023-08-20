package com.flipkart.varadhi.entities;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class TopicResource extends VaradhiResource {

    private static final String RESOURCE_TYPE_NAME = "TopicResource";

    @Size(min = 5, max = 50, message = "Project Length must be between 5 and 50")
    String project;

    @NotNull
    Boolean grouped;

    @NotNull
    Boolean exclusiveSubscription;

    CapacityPolicy capacityPolicy;

    //TODO::check if private constructor suffices.
    public TopicResource(
            String name,
            int version,
            String project,
            boolean grouped,
            boolean exclusiveSubscription,
            CapacityPolicy capacityPolicy
    ) {
        super(name, version);
        this.project = project;
        this.grouped = grouped;
        this.exclusiveSubscription = exclusiveSubscription;
        this.capacityPolicy = capacityPolicy;
    }
}
