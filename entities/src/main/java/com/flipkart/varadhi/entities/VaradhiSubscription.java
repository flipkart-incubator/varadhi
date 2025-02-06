package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Represents a subscription in the Varadhi.
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class VaradhiSubscription extends LifecycleEntity {

    private final String project;
    private final String topic;
    private String description;
    private boolean grouped;
    private Endpoint endpoint;
    private RetryPolicy retryPolicy;
    private ConsumptionPolicy consumptionPolicy;
    private SubscriptionShards shards;
    private Map<String, String> properties;

    private static final String SHARDS_ERROR = "Shards cannot be null or empty";
    private static final String PROPERTIES_ERROR = "Properties cannot be null or empty";

    /**
     * Constructs a new VaradhiSubscription instance.
     *
     * @param name              the name of the subscription
     * @param version           the version of the subscription
     * @param project           the project associated with the subscription
     * @param topic             the topic associated with the subscription
     * @param description       the description of the subscription
     * @param grouped           whether the subscription is grouped
     * @param endpoint          the endpoint of the subscription
     * @param retryPolicy       the retry policy of the subscription
     * @param consumptionPolicy the consumption policy of the subscription
     * @param shards            the shards of the subscription
     * @param status            the status of the subscription
     * @param properties        the properties of the subscription
     */
    private VaradhiSubscription(
            String name,
            int version,
            String project,
            String topic,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy,
            SubscriptionShards shards,
            LifecycleStatus status,
            Map<String, String> properties
    ) {
        super(name, version);
        this.project = validateNotNullOrEmpty(project, "Project");
        this.topic = validateNotNullOrEmpty(topic, "Topic");
        this.description = description;
        this.grouped = grouped;
        this.endpoint = endpoint;
        this.retryPolicy = retryPolicy;
        this.consumptionPolicy = consumptionPolicy;
        this.shards = validateShards(shards);
        this.status = status;
        this.properties = validateProperties(properties);
    }

    /**
     * Creates a new VaradhiSubscription instance.
     *
     * @param name              the name of the subscription
     * @param project           the project associated with the subscription
     * @param topic             the topic associated with the subscription
     * @param description       the description of the subscription
     * @param grouped           whether the subscription is grouped
     * @param endpoint          the endpoint of the subscription
     * @param retryPolicy       the retry policy of the subscription
     * @param consumptionPolicy the consumption policy of the subscription
     * @param shards            the shards of the subscription
     * @param properties        the properties of the subscription
     * @param actorCode         the actor code indicating the reason for the state
     *
     * @return a new VaradhiSubscription instance
     */
    public static VaradhiSubscription of(
            String name,
            String project,
            String topic,
            String description,
            boolean grouped,
            Endpoint endpoint,
            RetryPolicy retryPolicy,
            ConsumptionPolicy consumptionPolicy,
            SubscriptionShards shards,
            Map<String, String> properties,
            LifecycleStatus.ActorCode actorCode
    ) {
        return new VaradhiSubscription(
                name, INITIAL_VERSION, project, topic, description, grouped, endpoint, retryPolicy, consumptionPolicy,
                shards, new LifecycleStatus(LifecycleStatus.State.CREATING, actorCode), properties
        );
    }

    /**
     * Retrieves the integer value of a property.
     *
     * @param property the property name
     *
     * @return the integer value of the property
     *
     * @throws IllegalArgumentException if the property is not found or cannot be parsed as an integer
     */
    @JsonIgnore
    public int getIntProperty(String property) {
        String value = properties.get(property);
        if (value == null) {
            throw new IllegalArgumentException("Property not found: " + property);
        }
        return Integer.parseInt(value);
    }

    /**
     * Validates that a string is not null or empty.
     *
     * @param value     the string value to validate
     * @param fieldName the name of the field being validated
     *
     * @return the validated string value
     *
     * @throws IllegalArgumentException if the value is null or empty
     */
    private static String validateNotNullOrEmpty(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be null or empty");
        }
        return value;
    }

    /**
     * Validates that the shards are not null or empty.
     *
     * @param shards the shards to validate
     *
     * @return the validated shards
     *
     * @throws IllegalArgumentException if the shards are null or empty
     */
    private static SubscriptionShards validateShards(SubscriptionShards shards) {
        if (shards == null || shards.getShardCount() <= 0) {
            throw new IllegalArgumentException(SHARDS_ERROR);
        }
        return shards;
    }

    /**
     * Validates that the properties are not null or empty.
     *
     * @param properties the properties to validate
     *
     * @return the validated properties
     *
     * @throws IllegalArgumentException if the properties are null or empty
     */
    private static Map<String, String> validateProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            throw new IllegalArgumentException(PROPERTIES_ERROR);
        }
        return properties;
    }
}
