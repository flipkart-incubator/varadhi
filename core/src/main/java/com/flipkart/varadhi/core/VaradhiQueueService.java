package com.flipkart.varadhi.core;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
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
import static com.flipkart.varadhi.entities.web.QueueResource.getDefaultSubscriptionName;


/**
 * Service for queue CRUD, update, and restore. A queue is implemented as a topic plus a default
 * queue-style subscription (subscription name = {@link QueueResource#getDefaultSubscriptionName(String)}).
 * <p>
 * {@link #updateQueue} persists the topic leg from the body via {@link VaradhiTopicService#updateTopicState(VaradhiTopic)}
 * (merged with the stored topic’s version, lifecycle status, and internal storage topics), then updates the default
 * subscription (same contract as {@link VaradhiSubscriptionService#updateSubscription}).
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
        String queueName = queue.getName();

        String topicKey = topicFqn(projectName, queueName);
        String defaultSubscriptionName = getDefaultSubscriptionName(queueName);
        String subscriptionKey = subscriptionFqn(projectName, defaultSubscriptionName);

        if (varadhiTopicService.exists(topicKey) && varadhiSubscriptionService.exists(subscriptionKey)) {
            VaradhiSubscription existingSub = varadhiSubscriptionService.getSubscription(subscriptionKey);
            if (!topicKey.equals(existingSub.getTopic())) {
                throw new InvalidOperationForResourceException(
                    ("Cannot create queue '%s': the default subscription '%s' for this queue name already exists "
                     + "but is not attached to the queue's topic '%s'. Choose a different queue name, or remove "
                     + "or fix the conflicting subscription.").formatted(queueName, defaultSubscriptionName, topicKey)
                );
            }
            return get(projectName, queueName);
        }

        TopicResource topicResource = queue.toTopicResource(projectName, actionCode);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.getForQueue(project, topicResource);
        try {
            varadhiTopicService.create(varadhiTopic, project);
        } catch (DuplicateResourceException e) {
            VaradhiTopic existingTopic = varadhiTopicService.get(topicKey);
            if (existingTopic.getTopicCategory() != varadhiTopic.getTopicCategory()) {
                throw new InvalidOperationForResourceException(
                    ("Cannot create queue '%s': a topic with this name already exists as %s; "
                     + "queues require topic category QUEUE. Choose a different queue name.").formatted(
                         queueName,
                         existingTopic.getTopicCategory()
                     )
                );
            }
            if (existingTopic.isGrouped() != varadhiTopic.isGrouped()) {
                throw new InvalidOperationForResourceException(
                    ("Cannot create queue '%s': a topic with this name already exists with different ordering "
                     + "(existing grouped=%s, queue requests grouped=%s). Choose a different queue name.").formatted(
                         queueName,
                         existingTopic.isGrouped(),
                         varadhiTopic.isGrouped()
                     )
                );
            }
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
        String subFqn = subscriptionFqn(projectName, getDefaultSubscriptionName(queueName));

        VaradhiTopic topic = varadhiTopicService.get(topicFqn);
        VaradhiSubscription subscription = varadhiSubscriptionService.getSubscription(subFqn);
        return new QueueResult(topic, subscription);
    }

    /**
     * Whether the named topic is queue-backed: the topic exists and the default queue subscription
     * ({@link QueueResource#getDefaultSubscriptionName(String)}) exists and targets this topic.
     */
    public boolean isQueueBackedTopic(String projectName, String topicName) {
        String topicKey = topicFqn(projectName, topicName);
        if (!varadhiTopicService.exists(topicKey)) {
            return false;
        }
        VaradhiTopic topic = varadhiTopicService.get(topicKey);
//        topic.getTopicC
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
                                 subscriptionFqn(projectName, getDefaultSubscriptionName(topicName))
                             )
                         )
                         .toList();
    }

    /**
     * Updates a queue by updating its default subscription (endpoint, retry policy, grouping, consumption policy,
     * description). The request {@link QueueResource#getVersion()} is the expected subscription version for
     * optimistic concurrency, matching {@link com.flipkart.varadhi.entities.web.SubscriptionResource#getVersion()}
     * on subscription PUT.
     * <p>
     * Topic metadata is taken from {@link QueueResource#toTopicResource(String, LifecycleStatus.ActionCode)} and
     * written with {@link VaradhiTopicService#updateTopicState(VaradhiTopic)} before the subscription update so
     * grouping validation sees the latest topic.
     *
     * @param projectName  project from the path
     * @param queueName    queue name from the path (must equal {@link QueueResource#getName()} on the body)
     * @param queue        queue resource body
     * @param requestedBy identity for the controller-backed subscription update
     * @param actionCode  action code for building {@link SubscriptionResource} fields
     * @return the current topic and updated subscription
     */
    public CompletableFuture<QueueResult> updateQueue(
        String projectName,
        String queueName,
        QueueResource queue,
        String requestedBy,
        LifecycleStatus.ActionCode actionCode
    ) {
        validateQueueName(queue);
        if (!queueName.equals(queue.getName())) {
            throw new IllegalArgumentException("Queue name in path must match request body name.");
        }
        String topicKey = topicFqn(projectName, queueName);
        assertDefaultQueueLinked(projectName, queueName, "update");
        persistQueueTopicFromBody(topicKey, queue, projectName, actionCode);

        SubscriptionResource subRes = queue.toSubscriptionResource(projectName, actionCode);
        return varadhiSubscriptionService.updateSubscription(
            subscriptionFqn(projectName, getDefaultSubscriptionName(queueName)),
            queue.getVersion(),
            subRes.getDescription(),
            subRes.isGrouped(),
            subRes.getEndpointOptional().orElse(null),
            subRes.getRetryPolicy(),
            subRes.getConsumptionPolicy(),
            requestedBy
        ).thenApply(updated -> new QueueResult(varadhiTopicService.get(topicKey), updated));
    }

    /**
     * Deletes a queue: removes the default subscription asynchronously when required (controller-backed), then
     * removes the topic synchronously when that phase completes. Idempotent.
     */
    public CompletableFuture<Void> deleteQueue(
        String projectName,
        String queueName,
        Project project,
        String requestedBy,
        ResourceDeletionType deletionType,
        RequestActionType actionRequest
    ) {
        String defaultSubName = getDefaultSubscriptionName(queueName);
        String subFqn = subscriptionFqn(projectName, defaultSubName);

        CompletableFuture<Void> subscriptionPhase;
        if (!varadhiSubscriptionService.exists(subFqn)) {
            subscriptionPhase = CompletableFuture.completedFuture(null);
        } else {
            VaradhiSubscription sub = varadhiSubscriptionService.getSubscription(subFqn);
            boolean needSubscriptionDelete = deletionType == ResourceDeletionType.HARD_DELETE || sub.isActive();
            if (!needSubscriptionDelete) {
                subscriptionPhase = CompletableFuture.completedFuture(null);
            } else {
                subscriptionPhase = varadhiSubscriptionService.deleteSubscription(
                    subFqn,
                    project,
                    requestedBy,
                    deletionType,
                    actionRequest
                );
            }
        }

        return subscriptionPhase.thenRun(
            () -> deleteQueueTopicAfterSubscription(projectName, queueName, deletionType, actionRequest)
        );
    }

    private void deleteQueueTopicAfterSubscription(
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
     * Restores a queue: restores the default subscription asynchronously when required (controller-backed), then
     * restores the topic synchronously when that phase completes. Idempotent.
     */
    public CompletableFuture<Void> restoreQueue(
        String projectName,
        String queueName,
        String requestedBy,
        RequestActionType actionRequest
    ) {
        String defaultSubName = getDefaultSubscriptionName(queueName);
        String subFqn = subscriptionFqn(projectName, defaultSubName);
        String topicKey = topicFqn(projectName, queueName);

        assertDefaultQueueLinked(projectName, queueName, "restore");

        VaradhiSubscription sub = varadhiSubscriptionService.getSubscription(subFqn);
        VaradhiTopic topic = varadhiTopicService.get(topicKey);
        CompletableFuture<Void> subscriptionPhase;
        if (sub.isActive() && topic.isActive()) {
            subscriptionPhase = CompletableFuture.completedFuture(null);
        } else if (sub.isActive()) {
            subscriptionPhase = CompletableFuture.completedFuture(null);
        } else if (!sub.isInactive()) {
            throw new InvalidOperationForResourceException(
                "Subscription '%s' is not in a restorable inactive state.".formatted(defaultSubName)
            );
        } else {
            subscriptionPhase = varadhiSubscriptionService.restoreSubscription(subFqn, requestedBy, actionRequest)
                                                          .thenCompose(s -> CompletableFuture.completedFuture(null));
        }

        return subscriptionPhase.thenRun(
            () -> restoreQueueTopicAfterSubscription(projectName, queueName, actionRequest)
        );
    }

    private void restoreQueueTopicAfterSubscription(
        String projectName,
        String queueName,
        RequestActionType actionRequest
    ) {
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

    /**
     * Writes the queue body’s topic leg using {@link VaradhiTopicService#updateTopicState(VaradhiTopic)}. The argument
     * is {@code queue.toTopicResource(...).toVaradhiTopic(QUEUE)} merged with the stored topic’s version, lifecycle
     * status, and internal topics so the metastore is not reset to a creating/empty topic.
     */
    private void persistQueueTopicFromBody(
        String topicKey,
        QueueResource queue,
        String projectName,
        LifecycleStatus.ActionCode actionCode
    ) {
        VaradhiTopic stored = varadhiTopicService.get(topicKey);
        if (stored.getTopicCategory() != VaradhiTopic.TopicCategory.QUEUE) {
            throw new InvalidOperationForResourceException(
                "Cannot update queue '%s': underlying topic is not a queue topic.".formatted(queue.getName())
            );
        }
        VaradhiTopic updated = queue.toTopicResource(projectName, actionCode)
                                    .toVaradhiTopic(VaradhiTopic.TopicCategory.QUEUE);
        updated.setVersion(stored.getVersion() + 1);
        updated.setStatus(
            new LifecycleStatus(
                stored.getStatus().getState(),
                stored.getStatus().getMessage(),
                stored.getStatus().getActionCode()
            )
        );
        stored.getInternalTopics().forEach(updated::addInternalTopic);
        varadhiTopicService.updateTopicState(updated);
    }

    private void assertDefaultQueueLinked(String projectName, String queueName, String verb) {
        String defaultSubName = getDefaultSubscriptionName(queueName);
        String subFqn = subscriptionFqn(projectName, defaultSubName);
        String topicKey = topicFqn(projectName, queueName);

        if (!varadhiSubscriptionService.exists(subFqn)) {
            throw new ResourceNotFoundException(
                "Cannot %s queue '%s': default subscription '%s' not found.".formatted(verb, queueName, defaultSubName)
            );
        }
        if (!varadhiTopicService.exists(topicKey)) {
            throw new ResourceNotFoundException("Cannot %s queue '%s': topic not found.".formatted(verb, queueName));
        }
        VaradhiSubscription sub = varadhiSubscriptionService.getSubscription(subFqn);
        if (!topicKey.equals(sub.getTopic())) {
            throw new InvalidOperationForResourceException(
                "Subscription '%s' is not attached to topic for queue '%s'.".formatted(defaultSubName, queueName)
            );
        }
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
