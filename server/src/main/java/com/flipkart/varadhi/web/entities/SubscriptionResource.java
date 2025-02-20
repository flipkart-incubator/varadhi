package com.flipkart.varadhi.web.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.Validatable;
import com.flipkart.varadhi.entities.ValidateResource;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VersionedEntity;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a subscription resource in Varadhi.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Subscription name. Check naming constraints.", max = 64)
public class SubscriptionResource extends VersionedEntity implements Validatable {

    @NotBlank private final String project;

    @NotBlank private final String topic;

    @NotBlank private final String topicProject;

    @NotBlank private final String description;

    private final boolean grouped;

    @NotNull private final Endpoint endpoint;

    @NotNull private final RetryPolicy retryPolicy;

    @NotNull private final ConsumptionPolicy consumptionPolicy;

    @NotNull private final Map<String, String> properties;

    @Setter
    private LifecycleStatus.ActorCode actorCode;

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
     * @param actorCode         The actor code associated with the subscription.
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
        LifecycleStatus.ActorCode actorCode
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
        this.actorCode = actorCode;
    }

    /**
     * Creates a new SubscriptionResource instance.
     *
     * @param name              The name of the subscription.
     * @param project           The project associated with the subscription.
     * @param topic             The topic associated with the subscription.
     * @param topicProject      The project of the topic associated with the subscription.
     * @param description       The description of the subscription.
     * @param grouped           Indicates if the subscription is grouped.
     * @param endpoint          The endpoint associated with the subscription.
     * @param retryPolicy       The retry policy for the subscription.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @param properties        Additional properties for the subscription.
     * @param actorCode         The actor code associated with the subscription.
     *
     * @return A new SubscriptionResource instance.
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
        LifecycleStatus.ActorCode actorCode
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
            actorCode
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
        return String.join(NAME_SEPARATOR, project, subsResourceName);
    }

    /**
     * Creates a SubscriptionResource from a VaradhiSubscription.
     *
     * @param subscription The VaradhiSubscription instance.
     *
     * @return A new SubscriptionResource instance.
     */
    public static SubscriptionResource from(VaradhiSubscription subscription) {
        String[] subscriptionSegments = subscription.getName().split(NAME_SEPARATOR_REGEX);
        String subscriptionProject = subscriptionSegments[0];
        String subscriptionName = subscriptionSegments[1];

        String[] topicSegments = subscription.getTopic().split(NAME_SEPARATOR_REGEX);
        String topicProject = topicSegments[0];
        String topicName = topicSegments[1];

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
            subscription.getStatus().getActorCode()
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
