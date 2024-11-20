package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

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

    @NotNull
    Map<String, String> properties;

    private SubscriptionResource(
            String name,
            int version,
            String project,
            String topic,
            String topicProject,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy,
            Map<String, String> properties
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
        this.properties = null == properties ? new HashMap<>() : properties;
    }

    public static SubscriptionResource of(
            String name,
            String project,
            String topic,
            String topicProject,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy,
            Map<String, String> properties
    ) {
        return new SubscriptionResource(
                name, INITIAL_VERSION, project, topic, topicProject, description, grouped, endpoint, retryPolicy,
                consumptionPolicy, properties
        );
    }

    public static String buildInternalName(String project, String subsResourceName) {
        return String.join(NAME_SEPARATOR, project, subsResourceName);
    }

    public static SubscriptionResource from(VaradhiSubscription subscription) {
        String[] subscriptionNameSegments = subscription.getName().split(NAME_SEPARATOR_REGEX);
        String subscriptionProject = subscriptionNameSegments[0];
        String subscriptionName = subscriptionNameSegments[1];

        String[] topicNameSegments = subscription.getTopic().split(NAME_SEPARATOR_REGEX);
        String topicProject = topicNameSegments[0];
        String topicName = topicNameSegments[1];

        SubscriptionResource subResource = of(
                subscriptionName,
                subscriptionProject,
                topicName,
                topicProject,
                subscription.getDescription(),
                subscription.isGrouped(),
                subscription.getEndpoint(),
                subscription.getRetryPolicy(),
                subscription.getConsumptionPolicy(),
                subscription.getProperties()
        );
        subResource.setVersion(subscription.getVersion());
        return subResource;
    }

    @JsonIgnore
    public String getSubscriptionInternalName() {
        return buildInternalName(project, getName());
    }
}
