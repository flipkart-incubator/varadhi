package com.flipkart.varadhi.core;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR_REGEX;

/**
 * Service for queue CRUD and restore. A queue is implemented as a topic plus a default
 * queue-style subscription (subscription name = {@link QueueResource#getDefaultSubscriptionName(String)}).
 */
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
     * <p>
     * Idempotent: if the topic and default queue subscription already exist and are linked, returns
     * {@link #get(String, String)}.
     * <p>
     * If subscription creation fails after the topic was created, the topic may be left without a queue
     * subscription. Retrying this method with the same queue name does not recreate the topic; it only
     * runs subscription creation again, which should complete the queue (see also
     * {@link VaradhiSubscriptionService#createSubscription} for retriable subscription states).
     *
     * @param queue      the queue resource (name, project, grouped, etc.)
     * @param project    the project entity
     * @param actionCode the actor code for the operation
     * @return the created topic and subscription
     */
    public QueueResult create(QueueResource queue, Project project, LifecycleStatus.ActionCode actionCode) {
        validateQueueName(queue);
        String projectName = project.getName();
        String queueName = queue.getName().trim();
        queue.setName(queueName);

        String topicKey = topicFqn(projectName, queueName);
        String defaultSubscriptionName = QueueResource.getDefaultSubscriptionName(queueName);
        String subscriptionKey = subscriptionFqn(projectName, defaultSubscriptionName);

        if (varadhiTopicService.exists(topicKey) && varadhiSubscriptionService.exists(subscriptionKey)) {
            VaradhiSubscription existingSub = varadhiSubscriptionService.getSubscription(subscriptionKey);
            if (!topicKey.equals(existingSub.getTopic())) {
                throw new InvalidOperationForResourceException(
                    "Subscription '%s' already exists but is not attached to topic '%s'.".formatted(
                        defaultSubscriptionName,
                        queueName
                    )
                );
            }
            return get(projectName, queueName);
        }

        TopicResource topicResource = queue.toTopicResource(projectName, actionCode);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        if (!varadhiTopicService.exists(topicKey)) {
            varadhiTopicService.create(varadhiTopic, project);
        }

        VaradhiTopic createdTopic = varadhiTopicService.get(topicKey);
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
        List<String> topicNames = varadhiTopicService.list(projectName, includeInactive)
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
     * Subscription leg of queue delete: async (controller-backed), idempotent.
     * <p>
     * Call {@link #deleteQueueTopic(String, String, ResourceDeletionType, RequestActionType)} after this future
     * completes successfully. Skips when the default subscription is absent or already soft-deleted (for soft delete).
     */
    public CompletableFuture<Void> deleteQueueSubscription(
        String projectName,
        String queueName,
        Project project,
        String requestedBy,
        ResourceDeletionType deletionType,
        RequestActionType actionRequest
    ) {
        String defaultSubName = QueueResource.getDefaultSubscriptionName(queueName);
        String subFqn = subscriptionFqn(projectName, defaultSubName);
        String topicKey = topicFqn(projectName, queueName);

        boolean subPresent = varadhiSubscriptionService.exists(subFqn);
        boolean topicPresent = varadhiTopicService.exists(topicKey);
        if (!subPresent && !topicPresent) {
            return CompletableFuture.completedFuture(null);
        }
        if (!subPresent) {
            return CompletableFuture.completedFuture(null);
        }

        VaradhiSubscription sub = varadhiSubscriptionService.getSubscription(subFqn);
        boolean needSubscriptionDelete = deletionType == ResourceDeletionType.HARD_DELETE || sub.isActive();
        if (!needSubscriptionDelete) {
            return CompletableFuture.completedFuture(null);
        }
        return varadhiSubscriptionService.deleteSubscription(subFqn, project, requestedBy, deletionType, actionRequest);
    }

    /**
     * Topic leg of queue delete: synchronous meta/storage delete, idempotent.
     */
    public void deleteQueueTopic(
        String projectName,
        String queueName,
        ResourceDeletionType deletionType,
        RequestActionType actionRequest
    ) {
        String topicKey = topicFqn(projectName, queueName);
        if (!varadhiTopicService.exists(topicKey)) {
            return;
        }
        VaradhiTopic topic = varadhiTopicService.get(topicKey);
        boolean needTopicDelete = deletionType == ResourceDeletionType.HARD_DELETE || topic.isActive();
        if (needTopicDelete) {
            varadhiTopicService.delete(topicKey, deletionType, actionRequest);
        }
    }

    /**
     * Subscription leg of queue restore: async (controller-backed), idempotent.
     * <p>
     * Call {@link #restoreQueueTopic(String, String, RequestActionType)} after this future completes successfully.
     */
    public CompletableFuture<Void> restoreQueueSubscription(
        String projectName,
        String queueName,
        String requestedBy,
        RequestActionType actionRequest
    ) {
        String defaultSubName = QueueResource.getDefaultSubscriptionName(queueName);
        String subFqn = subscriptionFqn(projectName, defaultSubName);
        String topicKey = topicFqn(projectName, queueName);

        if (!varadhiSubscriptionService.exists(subFqn)) {
            throw new ResourceNotFoundException(
                "Cannot restore queue '%s': default subscription '%s' not found.".formatted(queueName, defaultSubName)
            );
        }
        if (!varadhiTopicService.exists(topicKey)) {
            throw new ResourceNotFoundException("Cannot restore queue '%s': topic not found.".formatted(queueName));
        }

        VaradhiSubscription sub = varadhiSubscriptionService.getSubscription(subFqn);
        if (!topicKey.equals(sub.getTopic())) {
            throw new InvalidOperationForResourceException(
                "Subscription '%s' is not attached to topic for queue '%s'.".formatted(defaultSubName, queueName)
            );
        }

        VaradhiTopic topic = varadhiTopicService.get(topicKey);
        if (sub.isActive() && topic.isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        if (sub.isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!sub.isInactive()) {
            throw new InvalidOperationForResourceException(
                "Subscription '%s' is not in a restorable inactive state.".formatted(defaultSubName)
            );
        }
        return varadhiSubscriptionService.restoreSubscription(subFqn, requestedBy, actionRequest)
                                         .thenCompose(s -> CompletableFuture.completedFuture(null));
    }

    /**
     * Topic leg of queue restore: synchronous meta restore, idempotent.
     */
    public void restoreQueueTopic(String projectName, String queueName, RequestActionType actionRequest) {
        String topicKey = topicFqn(projectName, queueName);
        if (!varadhiTopicService.exists(topicKey)) {
            throw new ResourceNotFoundException("Cannot restore queue '%s': topic not found.".formatted(queueName));
        }
        VaradhiTopic topic = varadhiTopicService.get(topicKey);
        if (topic.isActive()) {
            return;
        }
        if (!topic.isInactive()) {
            throw new InvalidOperationForResourceException(
                "Topic for queue '%s' is not in a restorable inactive state.".formatted(queueName)
            );
        }
        varadhiTopicService.restore(topicKey, actionRequest);
    }

    private static void validateQueueName(QueueResource queue) {
        if (queue.getName() == null || queue.getName().isBlank()) {
            throw new IllegalArgumentException("Queue name is required.");
        }
    }

    private static String topicFqn(String project, String queueOrTopicName) {
        return String.join(NAME_SEPARATOR, project, queueOrTopicName);
    }

    private static String subscriptionFqn(String project, String subscriptionName) {
        return SubscriptionResource.buildInternalName(project, subscriptionName);
    }
}
