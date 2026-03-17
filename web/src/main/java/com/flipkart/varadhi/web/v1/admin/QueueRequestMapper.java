package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.entities.web.request.SubscriptionRequestModel;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps QueueResource to TopicResource and SubscriptionResource for use in QueueHandlers.
 */
public final class QueueRequestMapper {

    private static final Endpoint DEFAULT_QUEUE_ENDPOINT = new Endpoint.HttpEndpoint(
        URI.create("http://localhost:8080"),
        "POST",
        "application/json",
        500,
        500,
        false
    );

    private static final RetryPolicy DEFAULT_QUEUE_RETRY_POLICY = new RetryPolicy(
        new com.flipkart.varadhi.entities.CodeRange[] {
            new com.flipkart.varadhi.entities.CodeRange(500, 502)
        },
        RetryPolicy.BackoffType.LINEAR,
        1,
        1,
        1,
        3
    );

    private static final ConsumptionPolicy DEFAULT_QUEUE_CONSUMPTION_POLICY =
        new ConsumptionPolicy(10, 1, 1, false, 1, null);

    private static final Map<String, String> DEFAULT_SUBSCRIPTION_PROPERTIES = new HashMap<>(
        Map.of(
            com.flipkart.varadhi.entities.Constants.SubscriptionProperties.UNSIDELINE_API_MESSAGE_COUNT,
            "100",
            com.flipkart.varadhi.entities.Constants.SubscriptionProperties.UNSIDELINE_API_GROUP_COUNT,
            "20",
            com.flipkart.varadhi.entities.Constants.SubscriptionProperties.GETMESSAGES_API_MESSAGES_LIMIT,
            "100"
        )
    );

    private QueueRequestMapper() {
    }

    /**
     * Converts queue resource to TopicResource for the given project and action code.
     */
    public static TopicResource toTopicResource(
        QueueResource queue,
        String project,
        LifecycleStatus.ActionCode actionCode
    ) {
        TopicResource topicResource = queue.toTopicResource(project, actionCode);
        topicResource.setActionCode(actionCode);
        return topicResource;
    }

    /**
     * Converts queue resource to SubscriptionResource for the given project.
     * Topic is assumed to be in the same project (queue name = topic name).
     */
    public static SubscriptionResource toSubscriptionResource(
        QueueResource queue,
        String project,
        LifecycleStatus.ActionCode actionCode
    ) {
        SubscriptionRequestModel subReq = queue.toSubscriptionRequest();
        String topicName = subReq.getTopicName();
        String description = "Queue subscription for " + topicName;
        boolean grouped = Boolean.TRUE.equals(subReq.getGrouped());
        RetryPolicy retryPolicy = subReq.getRetryPolicy() != null ? subReq.getRetryPolicy() : DEFAULT_QUEUE_RETRY_POLICY;
        return SubscriptionResource.of(
            subReq.getName(),
            project,
            topicName,
            project,
            description,
            grouped,
            DEFAULT_QUEUE_ENDPOINT,
            retryPolicy,
            DEFAULT_QUEUE_CONSUMPTION_POLICY,
            DEFAULT_SUBSCRIPTION_PROPERTIES,
            actionCode
        );
    }
}
