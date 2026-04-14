package com.flipkart.varadhi.entities.web;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varadhi.entities.CallbackConfig;
import com.flipkart.varadhi.entities.Constants;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.ValidateResource;
import com.flipkart.varadhi.entities.Validatable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Queue resource - base + queue-specific fields (topic + subscription aspects for a queue).
 */

@Getter
@Setter
@EqualsAndHashCode (callSuper = true)
@ValidateResource (message = "Invalid Queue name. Check naming constraints.", max = 64)
public class QueueResource extends BaseResource implements Validatable {
    private static final String QUEUE_SUBSCRIPTION_PREFIX = "sub_";

    // Queue extra fields (for topic part)
    /** primary zone for produce path (topic leg). */
    private String activeProduceZone;
    private TopicCapacityPolicy capacity;
    private LifecycleStatus.ActionCode actionCode;
    private String nfrFilterName;

    // Queue extra fields (for subscription part); parallelism is ConsumptionPolicy.maxParallelism on the subscription
    private RetryPolicy retryPolicy;
    private ConsumptionPolicy consumptionPolicy;
    private CallbackConfig callbackConfig;
    private Map<String, String> targetClientIds;

    /** Subscription properties (same semantics as {@link SubscriptionResource#getProperties()}). */
    private Map<String, String> properties;

    /**
     * Full constructor used for HTTP JSON binding: {@link JsonCreator} marks this as Jackson’s single property-based
     * creator so deserialization picks it even when other constructors exist (e.g. the 3-arg preset). Each
     * {@link JsonProperty} names the JSON field bound to that parameter; without them, Vert.x/Jackson may fail with
     * “no Creators exist” or choose the wrong constructor.
     */
    @JsonCreator
    public QueueResource(
        @JsonProperty ("name") String name,
        @JsonProperty ("version") int version,
        @JsonProperty ("project") String project,
        @JsonProperty ("secured") Boolean secured,
        @JsonProperty ("grouped") Boolean grouped,
        @JsonProperty ("appId") String appId,
        @JsonProperty ("nfrStrategy") String nfrStrategy,
        @JsonProperty ("activeProduceZone") String activeProduceZone,
        @JsonProperty ("capacity") TopicCapacityPolicy capacity,
        @JsonProperty ("actionCode") LifecycleStatus.ActionCode actionCode,
        @JsonProperty ("nfrFilterName") String nfrFilterName,
        @JsonProperty ("retryPolicy") RetryPolicy retryPolicy,
        @JsonProperty ("consumptionPolicy") ConsumptionPolicy consumptionPolicy,
        @JsonProperty ("callbackConfig") CallbackConfig callbackConfig,
        @JsonProperty ("targetClientIds") Map<String, String> targetClientIds,
        @JsonProperty ("properties") Map<String, String> properties
    ) {
        super(name, version);
        setProject(project);
        setSecured(Boolean.TRUE.equals(secured));
        setGrouped(Boolean.TRUE.equals(grouped));
        setAppId(appId);
        setNfrStrategy(nfrStrategy);
        this.activeProduceZone = activeProduceZone;
        this.capacity = capacity;
        this.actionCode = actionCode;
        this.nfrFilterName = nfrFilterName;
        this.retryPolicy = retryPolicy;
        this.consumptionPolicy = consumptionPolicy;
        this.callbackConfig = callbackConfig;
        this.targetClientIds = targetClientIds;
        this.properties = properties;
    }

    /**
     * Minimal construction with {@link Constants.QueueDefaults} for topic/subscription fields. {@code targetClientIds}
     * and {@code callbackConfig} stay unset until callers assign them.
     */
    public QueueResource(String name, int version, String project) {
        this(
            name,
            version,
            project,
            Constants.QueueDefaults.SECURED,
            Constants.QueueDefaults.GROUPED,
            Constants.QueueDefaults.APP_ID,
            Constants.QueueDefaults.NFR_STRATEGY,
            Constants.QueueDefaults.ACTIVE_PRODUCE_ZONE,
            Constants.QueueDefaults.defaultCapacity(),
            Constants.QueueDefaults.ACTION_CODE,
            Constants.QueueDefaults.NFR_FILTER_NAME,
            Constants.QueueDefaults.RETRY_POLICY,
            Constants.QueueDefaults.CONSUMPTION_POLICY,
            null,
            null,
            Constants.QueueDefaults.subscriptionPropertiesCopy()
        );
    }

    /**
     * Default subscription name for a queue (topic name with {@code sub_} prefix).
     *
     * @throws IllegalArgumentException if {@code queueOrTopicName} is null or blank
     */
    public static String getDefaultSubscriptionName(String queueOrTopicName) {
        if (queueOrTopicName == null || queueOrTopicName.isBlank()) {
            throw new IllegalArgumentException("Queue or topic name cannot be null or blank.");
        }
        return QUEUE_SUBSCRIPTION_PREFIX + queueOrTopicName;
    }

    /**
     * Builds TopicResource from this queue resource (topic part) for the given project and action code.
     */
    public TopicResource toTopicResource(String projectFromPath, LifecycleStatus.ActionCode actionCode) {
        String projectName = this.project != null && !this.project.isBlank() ? this.project : projectFromPath;
        TopicCapacityPolicy cap = this.capacity != null ? this.capacity : TopicCapacityPolicy.getDefault();
        LifecycleStatus.ActionCode code = actionCode != null ? actionCode : LifecycleStatus.ActionCode.USER_ACTION;
        String nfr = this.nfrFilterName != null ?
            this.nfrFilterName :
            this.getNfrStrategy() != null ? this.getNfrStrategy() : "";

        if (this.isGrouped()) {
            return TopicResource.grouped(this.getName(), projectName, cap, code, nfr);
        }
        return TopicResource.unGrouped(this.getName(), projectName, cap, code, nfr);
    }

    /**
     * Builds SubscriptionResource from this queue resource (subscription part) for the given project and action code.
     * Endpoint is omitted (optional on the subscription). Retry, consumption, and properties come from this resource;
     * HTTP handlers may fill omitted fields before calling the core service.
     */
    public SubscriptionResource toSubscriptionResource(String projectFromPath, LifecycleStatus.ActionCode actionCode) {
        String projectName = this.project != null && !this.project.isBlank() ? this.project : projectFromPath;
        String topicName = getName();
        String subName = getDefaultSubscriptionName(topicName);
        String description = "Queue subscription for " + topicName;
        LifecycleStatus.ActionCode code = actionCode != null ? actionCode : LifecycleStatus.ActionCode.USER_ACTION;
        return SubscriptionResource.of(
            subName,
            projectName,
            topicName,
            projectName,
            description,
            isGrouped(),
            null,
            retryPolicy,
            consumptionPolicy,
            properties,
            code,
            targetClientIds,
            callbackConfig
        );
    }
}
