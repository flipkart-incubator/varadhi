package com.flipkart.varadhi.entities.web;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents a subscription resource in Varadhi.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Subscription name. Check naming constraints.", max = 64)
public class SubscriptionResource extends Versioned implements Validatable {

    @NotBlank
    private final String project;

    @NotBlank
    private final String topic;

    @NotBlank
    private final String topicProject;

    @NotBlank
    private final String description;

    private final boolean grouped;

    @NotNull
    private final Endpoint endpoint;

    @NotNull
    private final RetryPolicy retryPolicy;

    @NotNull
    private final ConsumptionPolicy consumptionPolicy;

    @NotNull
    private final Map<String, String> properties;

    @Setter
    private LifecycleStatus.ActionCode actionCode;

    /**
     * Target client id per consumer endpoint: key = stable endpoint identifier (for HTTP consumers, commonly
     * {@link Endpoint.HttpEndpoint#getUri()}{@code .toString()} aligned with {@link #endpoint}), value = client id.
     * One entry is typical for topics; queues use one entry per logical endpoint. String keys are used instead of
     * {@link Endpoint} as map keys for a JSON-friendly contract; {@link #endpoint} carries the primary endpoint
     * configuration.
     */
    @NotNull
    private final Map<String, String> targetClientIds;

    /**
     * Constructs a new SubscriptionResource.
     *
     * @param name              The name of the subscription.
     * @param version           The version of the subscription.
     * @param project           The project associated with the subscription.
     * @param topic             The topic associated with the subscription.
     * @param topicProject      The project of the topic associated with the subscription.
     * @param description       The description of the subscription.
     * @param grouped           Indicates if the subscription is grouped.
     * @param endpoint          The endpoint associated with the subscription.
     * @param retryPolicy       The retry policy for the subscription.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @param properties        Additional properties for the subscription.
     * @param actionCode        The actor code associated with the subscription.
     * @param targetClientIds   Endpoint id → client id; at least one non-blank mapping required.
     */
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
        Map<String, String> properties,
        LifecycleStatus.ActionCode actionCode,
        Map<String, String> targetClientIds
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
        this.properties = properties == null ? new HashMap<>() : properties;
        this.actionCode = actionCode;
        this.targetClientIds = VaradhiSubscription.validateTargetClientIds(targetClientIds);
    }

    /**
     * Creates a new SubscriptionResource instance.
     *
     * @param targetClientIds   Endpoint id → client id; at least one non-blank mapping required.
     */
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
        Map<String, String> properties,
        LifecycleStatus.ActionCode actionCode,
        Map<String, String> targetClientIds
    ) {
        return new SubscriptionResource(
            name,
            INITIAL_VERSION,
            project,
            topic,
            topicProject,
            description,
            grouped,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            properties,
            actionCode,
            targetClientIds
        );
    }

    /**
     * Builds the internal name for the subscription.
     *
     * @param project          The project associated with the subscription.
     * @param subsResourceName The name of the subscription resource.
     *
     * @return The internal name for the subscription.
     */
    public static String buildInternalName(String project, String subsResourceName) {
        return VaradhiTopicName.of(project, subsResourceName).toFqn();
    }

    /**
     * Creates a SubscriptionResource from a VaradhiSubscription.
     *
     * @param subscription The VaradhiSubscription instance.
     *
     * @return A new SubscriptionResource instance.
     */
    public static SubscriptionResource from(VaradhiSubscription subscription) {
        VaradhiTopicName subscriptionFqn = VaradhiTopicName.parse(subscription.getName());
        String subscriptionProject = subscriptionFqn.getProjectName();
        String subscriptionName = subscriptionFqn.getTopicName();

        VaradhiTopicName topicFqn = VaradhiTopicName.parse(subscription.getTopic());
        String topicProject = topicFqn.getProjectName();
        String topicName = topicFqn.getTopicName();

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
            subscription.getProperties(),
            subscription.getStatus().getActionCode(),
            subscription.getTargetClientIds()
        );
        subResource.setVersion(subscription.getVersion());
        return subResource;
    }

    /**
     * Gets the internal name for the subscription.
     *
     * @return The internal name for the subscription.
     */
    @JsonIgnore
    public String getSubscriptionInternalName() {
        return buildInternalName(project, getName());
    }
}
