package com.flipkart.varadhi.entities;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
@ValidateResource(message = "Invalid Subscription name. Check naming constraints.", max = 64)
public class SubscriptionResource extends VersionedEntity implements Validatable {
    @NotBlank
    String project;

    @NotBlank
    String topic;

    @NotBlank
    String topicProject;

    @NotBlank
    String description;

    boolean grouped;

    @NotNull
    Endpoint endpoint;

    @NotNull
    RetryPolicy retryPolicy;

    @NotNull
    ConsumptionPolicy consumptionPolicy;

    public SubscriptionResource(
            String name,
            int version,
            String project,
            String topic,
            String topicProject,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy
    ) {
        super(name, version);
        this.project = project;
        this.topic = topic;
        this.topicProject = topicProject;
        this.description = description;
        this.grouped = grouped;
        this.endpoint = endpoint;
        this.retryPolicy = retryPolicy;
        this.consumptionPolicy = consumptionPolicy;
    }
}
