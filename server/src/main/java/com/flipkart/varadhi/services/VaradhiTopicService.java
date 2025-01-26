package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Service class for managing Varadhi topics.
 */
@Slf4j
public class VaradhiTopicService {

    private final StorageTopicService<StorageTopic> storageTopicService;
    private final MetaStore metaStore;

    /**
     * Constructs a VaradhiTopicService with the specified storage topic service and meta store.
     *
     * @param storageTopicService the storage topic service
     * @param metaStore           the meta store
     */
    public VaradhiTopicService(StorageTopicService<StorageTopic> storageTopicService, MetaStore metaStore) {
        this.storageTopicService = storageTopicService;
        this.metaStore = metaStore;
    }

    /**
     * Creates a new Varadhi topic.
     *
     * @param varadhiTopic the Varadhi topic to create
     * @param project the project associated with the topic
     */
    public void create(VaradhiTopic varadhiTopic, Project project) {
        log.info("Creating Varadhi topic: {}", varadhiTopic.getName());
        // Ensure StorageTopicService.create() is idempotent, allowing reuse of pre-existing topics.
        varadhiTopic.getInternalTopics().forEach((region, internalTopic) ->
                internalTopic.getActiveTopics().forEach(storageTopic ->
                        storageTopicService.create(storageTopic, project)
                )
        );
        metaStore.createTopic(varadhiTopic);
    }

    /**
     * Retrieves a Varadhi topic by its name.
     *
     * @param topicName the name of the topic
     * @return the Varadhi topic
     * @throws ResourceNotFoundException if the topic is not found or inactive
     */
    public VaradhiTopic get(String topicName) {
        VaradhiTopic varadhiTopic = metaStore.getTopic(topicName);

        if (!varadhiTopic.isActive()) {
            throw new ResourceNotFoundException("Topic %s not found.".formatted(topicName));
        }

        return varadhiTopic;
    }

    /**
     * Deletes a Varadhi topic by its name.
     *
     * @param topicName    the name of the topic
     * @param deletionType the type of deletion (hard or soft)
     */
    public void delete(String topicName, ResourceDeletionType deletionType) {
        log.info("Deleting Varadhi topic: {}", topicName);
        // TODO: If the only topic in a namespace, also delete the namespace and tenant. Perform cleanup independently of the delete operation.
        VaradhiTopic varadhiTopic = metaStore.getTopic(topicName);
        validateTopicForDeletion(topicName);

        if (deletionType.equals(ResourceDeletionType.HARD_DELETE)) {
            handleHardDelete(varadhiTopic);
        } else {
            handleSoftDelete(varadhiTopic);
        }
    }

    /**
     * Handles the soft deletion of a Varadhi topic.
     *
     * @param varadhiTopic the Varadhi topic to soft-delete
     */
    public void handleSoftDelete(VaradhiTopic varadhiTopic) {
        log.info("Soft deleting Varadhi topic: {}", varadhiTopic.getName());
        varadhiTopic.updateStatus(VaradhiTopic.Status.INACTIVE);
        metaStore.updateTopic(varadhiTopic);
    }

    /**
     * Handles the hard deletion of a Varadhi topic.
     *
     * @param varadhiTopic the Varadhi topic to hard delete
     */
    public void handleHardDelete(VaradhiTopic varadhiTopic) {
        log.info("Hard deleting Varadhi topic: {}", varadhiTopic.getName());

        Project project = metaStore.getProject(varadhiTopic.getProjectName());

        varadhiTopic.getInternalTopics().forEach((region, internalTopic) ->
                internalTopic.getActiveTopics().forEach(storageTopic ->
                        storageTopicService.delete(storageTopic.getName(), project)
                )
        );
        metaStore.deleteTopic(varadhiTopic.getName());
    }

    /**
     * Restores a deleted Varadhi topic.
     *
     * @param topicName the name of the topic to restore
     *
     * @throws InvalidOperationForResourceException if the topic is not deleted
     */
    public void restore(String topicName) {
        log.info("Restoring Varadhi topic: {}", topicName);

        VaradhiTopic varadhiTopic = metaStore.getTopic(topicName);

        if (varadhiTopic.isActive()) {
            throw new InvalidOperationForResourceException("Topic %s is not deleted.".formatted(topicName));
        }
        varadhiTopic.updateStatus(VaradhiTopic.Status.ACTIVE);
        metaStore.updateTopic(varadhiTopic);
    }

    /**
     * Validates if a topic can be deleted.
     *
     * @param topicName the name of the topic to validate
     *
     * @throws InvalidOperationForResourceException if the topic is being used by a subscription
     */
    private void validateTopicForDeletion(String topicName) {
        // TODO: Improve efficiency by avoiding a full scan of all subscriptions across projects.
        List<String> subscriptions = metaStore.getAllSubscriptionNames();
        boolean isTopicInUse = subscriptions.stream()
                .map(metaStore::getSubscription)
                .anyMatch(subscription -> subscription.getTopic().equals(topicName));

        if (isTopicInUse) {
            throw new InvalidOperationForResourceException(
                    "Cannot delete topic as it is being used by a subscription."
            );
        }
    }

    /**
     * Checks if a topic exists.
     *
     * @param topicName the name of the topic
     * @return true if the topic exists, false otherwise
     */
    public boolean exists(String topicName) {
        return metaStore.checkTopicExists(topicName);
    }

    /**
     * Retrieves a list of active Varadhi topics for a project.
     *
     * @param projectName the name of the project
     * @return a list of active Varadhi topic names
     */
    public List<String> getVaradhiTopics(String projectName) {
        return metaStore.getTopicNames(projectName).stream()
                .filter(topicName -> metaStore.getTopic(topicName).isActive())
                .toList();
    }
}
