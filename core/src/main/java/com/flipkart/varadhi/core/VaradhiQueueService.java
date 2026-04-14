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
     * Fully-qualified names for a queue: backing topic and default subscription within a project.
     */
    private record QueueFqn(String projectName, String queueName) {
        String defaultSubscriptionName() {
            return getDefaultSubscriptionName(queueName);
        }

        String topicFqn() {
            return String.join(NAME_SEPARATOR, projectName, queueName);
        }

        String subscriptionFqn() {
            return SubscriptionResource.buildInternalName(projectName, defaultSubscriptionName());
        }
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
        QueueFqn queueFqn = new QueueFqn(projectName, queueName);

        if (varadhiTopicService.exists(queueFqn.topicFqn()) && varadhiSubscriptionService.exists(
            queueFqn.subscriptionFqn()
        )) {
            assertDefaultQueueLinked(queueFqn, "create");
            return get(projectName, queueName);
        }

        TopicResource topicResource = queue.toTopicResource(projectName, actionCode);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource, VaradhiTopic.TopicCategory.QUEUE);
        try {
            varadhiTopicService.create(varadhiTopic, project);
        } catch (DuplicateResourceException e) {
            VaradhiTopic existingTopic = varadhiTopicService.get(queueFqn.topicFqn());
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

        VaradhiTopic createdTopic = varadhiTopicService.get(queueFqn.topicFqn());
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
        QueueFqn queueFqn = new QueueFqn(projectName, queueName);
        VaradhiTopic topic = varadhiTopicService.get(queueFqn.topicFqn());
        VaradhiSubscription subscription = varadhiSubscriptionService.getSubscription(queueFqn.subscriptionFqn());
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
                                 new QueueFqn(projectName, topicName).subscriptionFqn()
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
     * subscription update
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
        QueueFqn queueFqn = new QueueFqn(projectName, queueName);
        assertDefaultQueueLinked(queueFqn, "update");

        SubscriptionResource subRes = queue.toSubscriptionResource(projectName, actionCode);
        return varadhiSubscriptionService.updateSubscription(
            queueFqn.subscriptionFqn(),
            queue.getVersion(),
            subRes.getDescription(),
            subRes.isGrouped(),
            subRes.getEndpoint().orElse(null),
            subRes.getRetryPolicy(),
            subRes.getConsumptionPolicy(),
            requestedBy
        ).thenApply(updated -> new QueueResult(varadhiTopicService.get(queueFqn.topicFqn()), updated));
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
        QueueFqn queueFqn = new QueueFqn(projectName, queueName);

        CompletableFuture<Void> subscriptionPhase;
        if (!varadhiSubscriptionService.exists(queueFqn.subscriptionFqn())) {
            subscriptionPhase = CompletableFuture.completedFuture(null);
        } else {
            VaradhiSubscription sub = varadhiSubscriptionService.getSubscription(queueFqn.subscriptionFqn());
            boolean needSubscriptionDelete = deletionType == ResourceDeletionType.HARD_DELETE || sub.isActive();
            if (!needSubscriptionDelete) {
                subscriptionPhase = CompletableFuture.completedFuture(null);
            } else {
                subscriptionPhase = varadhiSubscriptionService.deleteSubscription(
                    queueFqn.subscriptionFqn(),
                    project,
                    requestedBy,
                    deletionType,
                    actionRequest
                );
            }
        }

        return subscriptionPhase.thenRun(
            () -> deleteQueueTopicAfterSubscription(queueFqn, deletionType, actionRequest)
        );
    }

    private void deleteQueueTopicAfterSubscription(
        QueueFqn queueFqn,
        ResourceDeletionType deletionType,
        RequestActionType actionRequest
    ) {
        final VaradhiTopic topic;
        try {
            topic = varadhiTopicService.get(queueFqn.topicFqn());
        } catch (ResourceNotFoundException e) {
            return;
        }
        boolean needTopicDelete = deletionType == ResourceDeletionType.HARD_DELETE || topic.isActive();
        if (needTopicDelete) {
            varadhiTopicService.delete(queueFqn.topicFqn(), deletionType, actionRequest);
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
        QueueFqn queueFqn = new QueueFqn(projectName, queueName);

        VaradhiSubscription sub = assertDefaultQueueLinked(queueFqn, "restore");
        VaradhiTopic topic = varadhiTopicService.get(queueFqn.topicFqn());
        CompletableFuture<Void> subscriptionPhase;
        // Subscription restore throws if not inactive (VaradhiSubscriptionService#restoreSubscription); keep
        // sub.isActive() branches so queue restore stays idempotent when the default sub is already active.
        if (sub.isActive() && topic.isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        if (sub.isActive()) {
            return CompletableFuture.completedFuture(null);
        } else {
            subscriptionPhase = varadhiSubscriptionService.restoreSubscription(
                queueFqn.subscriptionFqn(),
                requestedBy,
                actionRequest
            ).thenCompose(s -> CompletableFuture.completedFuture(null));
        }

        return subscriptionPhase.thenRun(() -> restoreQueueTopicAfterSubscription(queueFqn, actionRequest));
    }

    private void restoreQueueTopicAfterSubscription(QueueFqn queueFqn, RequestActionType actionRequest) {
        final VaradhiTopic topic;
        try {
            topic = varadhiTopicService.get(queueFqn.topicFqn());
        } catch (ResourceNotFoundException e) {
            throw new ResourceNotFoundException(
                "Cannot restore queue '%s': topic not found.".formatted(queueFqn.queueName())
            );
        }
        if (!topic.isInactive()) {
            throw new InvalidOperationForResourceException(
                "Topic for queue '%s' is not in a restorable inactive state.".formatted(queueFqn.queueName())
            );
        }
        varadhiTopicService.restore(queueFqn.topicFqn(), actionRequest);
    }

    /**
     * Ensures the default queue subscription and backing topic exist and reference each other.
     *
     * @return the loaded default subscription (caller may reuse to avoid a second metastore read)
     */
    private VaradhiSubscription assertDefaultQueueLinked(QueueFqn queueFqn, String verb) {
        if (!varadhiSubscriptionService.exists(queueFqn.subscriptionFqn())) {
            throw new ResourceNotFoundException(
                "Cannot %s queue '%s': default subscription '%s' not found.".formatted(
                    verb,
                    queueFqn.queueName(),
                    queueFqn.defaultSubscriptionName()
                )
            );
        }
        if (!varadhiTopicService.exists(queueFqn.topicFqn())) {
            throw new ResourceNotFoundException(
                "Cannot %s queue '%s': topic not found.".formatted(verb, queueFqn.queueName())
            );
        }
        VaradhiSubscription sub = varadhiSubscriptionService.getSubscription(queueFqn.subscriptionFqn());
        if (!queueFqn.topicFqn().equals(sub.getTopic())) {
            throw new InvalidOperationForResourceException(
                "Subscription '%s' is not attached to topic for queue '%s'.".formatted(
                    queueFqn.defaultSubscriptionName(),
                    queueFqn.queueName()
                )
            );
        }
        return sub;
    }

    private static void validateQueueName(QueueResource queue) {
        if (queue.getName() == null || queue.getName().isBlank()) {
            throw new IllegalArgumentException("Queue name is required.");
        }
    }
}
