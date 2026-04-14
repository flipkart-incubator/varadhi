package com.flipkart.varadhi.entities.web;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.varadhi.entities.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents a subscription resource in Varadhi.
 */
@Getter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Subscription name. Check naming constraints.", max = 64)
public class SubscriptionResource extends BaseResource implements Validatable {

    @NotBlank
    private final String topic;

    @NotBlank
    private final String topicProject;

    @NotBlank
    private final String description;

    /** Optional; when absent at runtime, delivery may use {@link VaradhiSubscription#resolveDeliveryEndpoint()}. */
    @Getter (AccessLevel.NONE)
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
     * {@link Endpoint.HttpEndpoint#getUri()}{@code .toString()} aligned with {@link #getEndpoint()} when
     * present), value = client id. One entry is typical for topics; queues use one entry per logical endpoint. When
     * no explicit endpoint is set, the callback URL may appear only in these keys (see
     * {@link VaradhiSubscription#resolveDeliveryEndpoint()}).
     */
    @NotNull
    private final Map<String, String> targetClientIds;

    /**
     * Optional HTTP callback code-range policy for queue-style subscriptions; {@code null} if not used.
     */
    private final CallbackConfig callbackConfig;

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
     * @param endpoint          The endpoint associated with the subscription ({@code null} if not specified).
     * @param retryPolicy       The retry policy for the subscription.
     * @param consumptionPolicy The consumption policy for the subscription.
     * @param properties        Additional properties for the subscription.
     * @param actionCode        The actor code associated with the subscription.
     * @param targetClientIds   Endpoint id → client id; at least one non-blank mapping required.
     * @param callbackConfig    optional callback config ({@code null} for topic-style subscriptions).
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
        Map<String, String> targetClientIds,
        CallbackConfig callbackConfig
    ) {
        super(name, version);
        setProject(project);
        setGrouped(grouped);
        this.topic = topic;
        this.topicProject = topicProject;
        this.description = description;
        this.endpoint = endpoint;
        this.retryPolicy = retryPolicy;
        this.consumptionPolicy = consumptionPolicy;
        this.properties = properties == null ? new HashMap<>() : properties;
        this.actionCode = actionCode;
        this.targetClientIds = VaradhiSubscription.validateTargetClientIds(targetClientIds);
        this.callbackConfig = callbackConfig;
    }

    /**
     * Endpoint can be null in case of queue
     * @return Optional of endpoint
     */
    @JsonGetter ("endpoint")
    public Optional<Endpoint> getEndpoint() {
        return Optional.ofNullable(endpoint);
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
        return of(
            name,
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
            targetClientIds,
            null
        );
    }

    /**
     * Same as {@link #of(String, String, String, String, String, boolean, Endpoint, RetryPolicy, ConsumptionPolicy, Map, LifecycleStatus.ActionCode, Map)}
     * with optional {@link CallbackConfig} for queue-style subscriptions.
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
        Map<String, String> targetClientIds,
        CallbackConfig callbackConfig
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
            targetClientIds,
            callbackConfig
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
            subscription.getEndpoint().orElse(null),
            subscription.getRetryPolicy(),
            subscription.getConsumptionPolicy(),
            subscription.getProperties(),
            subscription.getStatus().getActionCode(),
            subscription.getTargetClientIds(),
            subscription.getCallbackConfig()
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
        return buildInternalName(getProject(), getName());
    }
}
