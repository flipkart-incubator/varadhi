package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;

/**
 * Maps QueueResource to TopicResource and SubscriptionResource for use in QueueHandlers.
 */
public final class QueueRequestMapper {
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
     * Delegates to {@link QueueResource#toSubscriptionResource(String, LifecycleStatus.ActionCode)}.
     */
    public static SubscriptionResource toSubscriptionResource(
        QueueResource queue,
        String project,
        LifecycleStatus.ActionCode actionCode
    ) {
        return queue.toSubscriptionResource(project, actionCode);
    }
}
