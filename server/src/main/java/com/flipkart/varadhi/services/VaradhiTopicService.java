package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.ProjectStore;
import com.flipkart.varadhi.spi.db.SubscriptionStore;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.web.entities.ResourceActionRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Service class for managing Varadhi topics.
 */
@Slf4j
public class VaradhiTopicService {

    private final StorageTopicService<StorageTopic> storageTopicService;
    private final TopicStore topicStore;
    private final SubscriptionStore subscriptionStore;
    private final ProjectStore projectStore;

    /**
     * Constructs a VaradhiTopicService with the specified storage topic service and meta store.
     *
     * @param storageTopicService the storage topic service
     * @param topicStore           the meta store
     */
    public VaradhiTopicService(
        StorageTopicService<StorageTopic> storageTopicService,
        TopicStore topicStore,
        SubscriptionStore subscriptionStore,
        ProjectStore projectStore
    ) {
        this.storageTopicService = storageTopicService;
        this.topicStore = topicStore;
        this.subscriptionStore = subscriptionStore;
        this.projectStore = projectStore;
    }

    /**
     * Creates a new Varadhi topic.
     *
     * @param varadhiTopic the Varadhi topic to create
     * @param project      the project associated with the topic
     */
    public void create(VaradhiTopic varadhiTopic, Project project) {
        log.info("Creating Varadhi topic: {}", varadhiTopic.getName());
        try {
            if (!exists(varadhiTopic.getName())) {
                topicStore.create(varadhiTopic);
            } else {
                VaradhiTopic existingTopic = get(varadhiTopic.getName());
                if (!existingTopic.isRetriable()) {
                    throw new DuplicateResourceException(
                        String.format("Topic '%s' already exists.", varadhiTopic.getName())
                    );
                }
                topicStore.update(varadhiTopic);
            }

            createStorageTopics(varadhiTopic, project);
            varadhiTopic.markCreated();
        } catch (Exception e) {
            varadhiTopic.markCreateFailed(e.getMessage());
            throw e;
        } finally {
            updateTopicState(varadhiTopic);
        }
    }

    /**
     * Creates storage topics for a Varadhi topic.
     *
     * @param varadhiTopic the Varadhi topic
     * @param project      the project associated with the topic
     */
    private void createStorageTopics(VaradhiTopic varadhiTopic, Project project) {
        // Ensure StorageTopicService.create() is idempotent, allowing reuse of pre-existing topics.
        varadhiTopic.getInternalTopics()
                    .forEach(
                        (region, internalTopic) -> internalTopic.getActiveTopics()
                                                                .forEach(
                                                                    storageTopic -> storageTopicService.create(
                                                                        storageTopic,
                                                                        project
                                                                    )
                                                                )
                    );
    }

    /**
     * Retrieves a Varadhi topic by its name.
     *
     * @param topicName the name of the topic
     *
     * @return the Varadhi topic
     */
    public VaradhiTopic get(String topicName) {
        return topicStore.get(topicName);
    }

    /**
     * Deletes a Varadhi topic by its name.
     *
     * @param topicName     the name of the topic to delete
     * @param deletionType  the type of deletion (hard or soft)
     * @param actionRequest the request containing the action code and message for the deletion
     */
    public void delete(String topicName, ResourceDeletionType deletionType, ResourceActionRequest actionRequest) {
        log.info("Deleting Varadhi topic: {}", topicName);
        // TODO: If the only topic in a namespace, also delete the namespace and tenant. Perform cleanup independently of the delete operation.
        VaradhiTopic varadhiTopic = topicStore.get(topicName);
        validateTopicForDeletion(topicName, deletionType);

        if (deletionType.equals(ResourceDeletionType.HARD_DELETE)) {
            handleHardDelete(varadhiTopic, actionRequest);
        } else {
            handleSoftDelete(varadhiTopic, actionRequest);
        }
    }

    /**
     * Handles the soft deletion of a Varadhi topic.
     *
     * @param varadhiTopic  the Varadhi topic to be soft-deleted
     * @param actionRequest the request containing the actor code and message for the soft deletion
     */
    public void handleSoftDelete(VaradhiTopic varadhiTopic, ResourceActionRequest actionRequest) {
        log.info("Soft deleting Varadhi topic: {}", varadhiTopic.getName());
        varadhiTopic.markInactive(actionRequest.actorCode(), actionRequest.message());
        topicStore.update(varadhiTopic);
    }

    /**
     * Handles the hard deletion of a Varadhi topic.
     *
     * @param varadhiTopic  the Varadhi topic to hard delete
     * @param actionRequest the request containing the actor code and message for the deletion
     */
    public void handleHardDelete(VaradhiTopic varadhiTopic, ResourceActionRequest actionRequest) {
        log.info("Hard deleting Varadhi topic: {}", varadhiTopic.getName());

        Project project = projectStore.get(varadhiTopic.getProjectName());

        try {
            varadhiTopic.markDeleting(actionRequest.actorCode(), "Starting Topic Deletion");
            topicStore.update(varadhiTopic);

            varadhiTopic.getInternalTopics()
                        .forEach(
                            (region, internalTopic) -> internalTopic.getActiveTopics()
                                                                    .forEach(
                                                                        storageTopic -> storageTopicService.delete(
                                                                            storageTopic.getName(),
                                                                            project
                                                                        )
                                                                    )
                        );
            topicStore.delete(varadhiTopic.getName());
        } catch (Exception e) {
            varadhiTopic.markDeleteFailed(e.getMessage());
            updateTopicState(varadhiTopic);
            throw e;
        }
    }

    /**
     * Restores a deleted Varadhi topic.
     *
     * @param topicName     the name of the topic to restore
     * @param actionRequest the request containing the actor code and message for the restoration
     *
     * @throws InvalidOperationForResourceException if the topic is not deleted or if the restoration is not allowed
     */
    public void restore(String topicName, ResourceActionRequest actionRequest) {
        log.info("Restoring Varadhi topic: {}", topicName);

        VaradhiTopic varadhiTopic = topicStore.get(topicName);

        if (varadhiTopic.isActive()) {
            throw new InvalidOperationForResourceException("Topic %s is not deleted.".formatted(topicName));
        }

        if (!varadhiTopic.isInactive()) {
            throw new InvalidOperationForResourceException("Only inactive topics can be restored.");
        }

        LifecycleStatus.ActorCode lastAction = varadhiTopic.getStatus().getActorCode();
        boolean isVaradhiAdmin = actionRequest.actorCode() == LifecycleStatus.ActorCode.SYSTEM_ACTION || actionRequest
                                                                                                                      .actorCode()
                                                                                                         == LifecycleStatus.ActorCode.ADMIN_ACTION;

        if (!lastAction.isUserAllowed() && !isVaradhiAdmin) {
            throw new InvalidOperationForResourceException(
                "Restoration denied. Only Varadhi Admin can restore this topic."
            );
        }

        varadhiTopic.restore(actionRequest.actorCode(), actionRequest.message());
        topicStore.update(varadhiTopic);
    }

    /**
     * Validates if a topic can be deleted based on the deletion type.
     *
     * @param topicName    the name of the topic to validate
     * @param deletionType the type of deletion (SOFT_DELETE or HARD_DELETE)
     *
     * @throws InvalidOperationForResourceException if the topic cannot be deleted
     */
    private void validateTopicForDeletion(String topicName, ResourceDeletionType deletionType) {
        // TODO: Improve efficiency by avoiding a full scan of all subscriptions across projects.
        List<VaradhiSubscription> subscriptions = subscriptionStore.getAllNames()
                                                                   .stream()
                                                                   .map(subscriptionStore::get)
                                                                   .filter(s -> s.getTopic().equals(topicName))
                                                                   .toList();

        if (subscriptions.isEmpty()) {
            return;
        }

        boolean hasActiveSubscriptions = subscriptions.stream()
                                                      .anyMatch(
                                                          s -> s.getStatus().getState() == LifecycleStatus.State.CREATED
                                                      );

        if (deletionType == ResourceDeletionType.SOFT_DELETE && hasActiveSubscriptions) {
            throw new InvalidOperationForResourceException("Cannot delete topic as it has active subscriptions.");
        }

        if (deletionType == ResourceDeletionType.HARD_DELETE) {
            throw new InvalidOperationForResourceException("Cannot delete topic as it has existing subscriptions.");
        }
    }

    /**
     * Checks if a topic exists.
     *
     * @param topicName the name of the topic
     *
     * @return true if the topic exists, false otherwise
     */
    public boolean exists(String topicName) {
        return topicStore.exists(topicName);
    }

    /**
     * Retrieves a list of Varadhi topic names for a given project.
     *
     * @param projectName     the name of the project
     * @param includeInactive flag to include inactive or soft-deleted topics
     *
     * @return a list of Varadhi topic names
     */
    public List<String> getVaradhiTopics(String projectName, boolean includeInactive) {
        return topicStore.getAllNames(projectName)
                         .stream()
                         .filter(topicName -> includeInactive || topicStore.get(topicName).isActive())
                         .toList();
    }

    /**
     * Updates the state of the given Varadhi topic in the meta store.
     *
     * @param varadhiTopic the Varadhi topic whose state is to be updated
     */
    private void updateTopicState(VaradhiTopic varadhiTopic) {
        try {
            topicStore.update(varadhiTopic);
        } catch (Exception e) {
            log.error("Failed to update topic state: {}", varadhiTopic.getName(), e);
        }
    }
}
