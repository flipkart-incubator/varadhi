package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.core.subscription.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.core.topic.VaradhiTopicFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR_REGEX;

/**
 * Service for queue CRUD and restore. A queue is implemented as a topic plus a default
 * queue-style subscription (subscription name = {@link QueueResource#getDefaultSubscriptionName(String)}).
 */
@Slf4j
public class VaradhiQueueService {

    private final VaradhiTopicFactory varadhiTopicFactory;
    private final VaradhiTopicService varadhiTopicService;
    private final VaradhiSubscriptionService varadhiSubscriptionService;
    private final VaradhiSubscriptionFactory varadhiSubscriptionFactory;

    public VaradhiQueueService(
        VaradhiTopicFactory varadhiTopicFactory,
        VaradhiTopicService varadhiTopicService,
        VaradhiSubscriptionService varadhiSubscriptionService,
        VaradhiSubscriptionFactory varadhiSubscriptionFactory
    ) {
        this.varadhiTopicFactory = varadhiTopicFactory;
        this.varadhiTopicService = varadhiTopicService;
        this.varadhiSubscriptionService = varadhiSubscriptionService;
        this.varadhiSubscriptionFactory = varadhiSubscriptionFactory;
    }

    /**
     * Result of queue create or get: the underlying topic and subscription.
     */
    public record QueueResult(VaradhiTopic topic, VaradhiSubscription subscription) {
    }

    /**
     * Creates a queue (topic + default subscription) for the given project and action code.
     *
     * @param queue      the queue resource (name, project, grouped, etc.)
     * @param project    the project entity
     * @param actionCode the actor code for the operation
     * @return the created topic and subscription
     */
    public QueueResult create(QueueResource queue, Project project, LifecycleStatus.ActionCode actionCode) {
        validateQueueName(queue);
        String projectName = project.getName();
        String queueName = queue.getName() != null ? queue.getName().trim() : "";

        TopicResource topicResource = queue.toTopicResource(projectName, actionCode);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        varadhiTopicService.create(varadhiTopic, project);

        VaradhiTopic createdTopic = varadhiTopicService.get(topicFqn(projectName, queueName));
        SubscriptionResource subscriptionResource = queue.toSubscriptionResource(projectName, actionCode);
        VaradhiSubscription varadhiSubscription = varadhiSubscriptionFactory.get(
            subscriptionResource,
            project,
            createdTopic
        );
        VaradhiSubscription createdSubscription = varadhiSubscriptionService.createSubscription(
            createdTopic,
            varadhiSubscription,
            project
        );

        return new QueueResult(createdTopic, createdSubscription);
    }

    /**
     * Gets a queue by project and queue name (returns the topic and default subscription).
     */
    public QueueResult get(String projectName, String queueName) {
        String topicFqn = topicFqn(projectName, queueName);
        String subFqn = subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(queueName));

        VaradhiTopic topic = varadhiTopicService.get(topicFqn);
        VaradhiSubscription subscription = varadhiSubscriptionService.getSubscription(subFqn);
        return new QueueResult(topic, subscription);
    }

    /**
     * Lists queue names in the project (topics that have the default queue subscription).
     */
    public List<String> list(String projectName, boolean includeInactive) {
        List<String> topicNames = varadhiTopicService.getVaradhiTopics(projectName, includeInactive)
                                                     .stream()
                                                     .filter(topic -> topic.startsWith(projectName + NAME_SEPARATOR))
                                                     .map(topic -> topic.split(NAME_SEPARATOR_REGEX)[1])
                                                     .toList();

        Set<String> subscriptionNames = Set.copyOf(
            varadhiSubscriptionService.getSubscriptionList(projectName, includeInactive)
        );
        return topicNames.stream()
                         .filter(
                             topicName -> subscriptionNames.contains(
                                 subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(topicName))
                             )
                         )
                         .toList();
    }

    /**
     * Deletes a queue (subscription first, then topic).
     */
    public void delete(
        String projectName,
        String queueName,
        Project project,
        String requestedBy,
        ResourceDeletionType deletionType,
        RequestActionType actionRequest
    ) {
        String subFqn = subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(queueName));
        String topicFqn = topicFqn(projectName, queueName);

        CompletableFuture<Void> deleteSub = varadhiSubscriptionService.deleteSubscription(
            subFqn,
            project,
            requestedBy,
            deletionType,
            actionRequest
        );
        deleteSub.join();

        varadhiTopicService.delete(topicFqn, deletionType, actionRequest);
    }

    /**
     * Restores a queue (subscription first, then topic).
     */
    public void restore(String projectName, String queueName, String requestedBy, RequestActionType actionRequest) {
        String subFqn = subscriptionFqn(projectName, QueueResource.getDefaultSubscriptionName(queueName));
        String topicFqn = topicFqn(projectName, queueName);

        CompletableFuture<VaradhiSubscription> restoreSub = varadhiSubscriptionService.restoreSubscription(
            subFqn,
            requestedBy,
            actionRequest
        );
        restoreSub.join();

        varadhiTopicService.restore(topicFqn, actionRequest);
    }

    private static void validateQueueName(QueueResource queue) {
        if (queue.getName() == null || queue.getName().isBlank()) {
            throw new IllegalArgumentException("Queue name is required.");
        }
        if (queue.getTeam() != null && !queue.getTeam().isBlank()) {
            // optional: validate team exists if needed
        }
    }

    private static String topicFqn(String project, String queueOrTopicName) {
        return String.join(NAME_SEPARATOR, project, queueOrTopicName);
    }

    private static String subscriptionFqn(String project, String subscriptionName) {
        return SubscriptionResource.buildInternalName(project, subscriptionName);
    }
}
