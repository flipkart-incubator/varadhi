package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.CallbackConfig;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.Validatable;
import com.flipkart.varadhi.entities.CodeRange;
import com.flipkart.varadhi.entities.Constants;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.net.URI;
import java.util.Map;

/**
 * Queue resource - base + queue-specific fields (topic + subscription aspects for a queue).
 */

@Getter
@Setter
@EqualsAndHashCode (callSuper = true)
public class QueueResource extends BaseResource implements Validatable {
    private static final String QUEUE_SUBSCRIPTION_PREFIX = "sub_";

    private static final Endpoint DEFAULT_QUEUE_ENDPOINT = new Endpoint.HttpEndpoint(
        URI.create("http://localhost:8080"),
        "POST",
        "application/json",
        500,
        500,
        false
    );

    private static final RetryPolicy DEFAULT_QUEUE_RETRY_POLICY = new RetryPolicy(
        new CodeRange[] {new CodeRange(500, 502)},
        RetryPolicy.BackoffType.LINEAR,
        1,
        1,
        1,
        3
    );

    private static final ConsumptionPolicy DEFAULT_QUEUE_CONSUMPTION_POLICY = new ConsumptionPolicy(
        10,
        1,
        1,
        false,
        1,
        null
    );

    private static final Map<String, String> DEFAULT_SUBSCRIPTION_PROPERTIES = Map.of(
        Constants.SubscriptionProperties.UNSIDELINE_API_MESSAGE_COUNT,
        "100",
        Constants.SubscriptionProperties.UNSIDELINE_API_GROUP_COUNT,
        "20",
        Constants.SubscriptionProperties.GETMESSAGES_API_MESSAGES_LIMIT,
        "100"
    );

    // Queue extra fields (for topic part)
    private String activeProduceZone;
    private TopicCapacityPolicy capacity;
    private LifecycleStatus.ActionCode actionCode;
    private String nfrFilterName;

    // Queue extra fields (for subscription part)
    private Integer noOfConsumers;
    private RetryPolicy retryPolicy;
    private CallbackConfig callbackConfig;
    private Map<String, String> targetClientIds;

    public QueueResource(
        String name,
        int version,
        String project,
        Boolean secured,
        Boolean grouped,
        String appId,
        String nfrStrategy,
        String activeProduceZone,
        TopicCapacityPolicy capacity,
        LifecycleStatus.ActionCode actionCode,
        String nfrFilterName,
        Integer noOfConsumers,
        RetryPolicy retryPolicy,
        CallbackConfig callbackConfig,
        Map<String, String> targetClientIds
    ) {
        super(name, version);
        setProject(project);
        setSecured(secured);
        setGrouped(grouped);
        setAppId(appId);
        setNfrStrategy(nfrStrategy);
        this.activeProduceZone = activeProduceZone;
        this.capacity = capacity;
        this.actionCode = actionCode;
        this.nfrFilterName = nfrFilterName;
        this.noOfConsumers = noOfConsumers;
        this.retryPolicy = retryPolicy;
        this.callbackConfig = callbackConfig;
        this.targetClientIds = targetClientIds;
    }

    /**
     * Default subscription name for a queue (topic name + suffix).
     */
    public static String getDefaultSubscriptionName(String queueOrTopicName) {
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
            (this.getNfrStrategy() != null ? this.getNfrStrategy() : "");

        if (Boolean.TRUE.equals(this.getGrouped())) {
            return TopicResource.grouped(this.getName(), projectName, cap, code, nfr);
        }
        return TopicResource.unGrouped(this.getName(), projectName, cap, code, nfr);
    }

    /**
     * Builds SubscriptionResource from this queue resource (subscription part) for the given project and action code.
     * Uses default endpoint, retry policy, consumption policy, and properties for the queue-style subscription.
     */
    public SubscriptionResource toSubscriptionResource(String projectFromPath, LifecycleStatus.ActionCode actionCode) {
        String projectName = this.project != null && !this.project.isBlank() ? this.project : projectFromPath;
        String topicName = getName();
        String subName = getDefaultSubscriptionName(topicName);
        String description = "Queue subscription for " + topicName;
        boolean grouped = Boolean.TRUE.equals(getGrouped());
        LifecycleStatus.ActionCode code = actionCode != null ? actionCode : LifecycleStatus.ActionCode.USER_ACTION;
        return SubscriptionResource.of(
            subName,
            projectName,
            topicName,
            projectName,
            description,
            grouped,
            DEFAULT_QUEUE_ENDPOINT,
            retryPolicy != null ? retryPolicy : DEFAULT_QUEUE_RETRY_POLICY,
            DEFAULT_QUEUE_CONSUMPTION_POLICY,
            DEFAULT_SUBSCRIPTION_PROPERTIES,
            code,
            targetClientIds
        );
    }
}
