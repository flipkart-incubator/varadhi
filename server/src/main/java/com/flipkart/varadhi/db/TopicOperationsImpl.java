package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.topic.TopicOperations;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.TOPIC;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;

public class TopicOperationsImpl implements TopicOperations {
    private final ZKMetaStore zkMetaStore;

    public TopicOperationsImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
    }
    /**
     * Creates a new topic.
     *
     * @param topic the topic to create
     * @throws IllegalArgumentException   if topic is null or invalid
     * @throws DuplicateResourceException if topic already exists
     * @throws ResourceNotFoundException  if associated project doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createTopic(VaradhiTopic topic) {
        ZNode znode = ZNode.ofTopic(topic.getName());
        zkMetaStore.createTrackedZNodeWithData(znode, topic, ResourceType.TOPIC);
    }

    /**
     * Retrieves a topic by its name.
     *
     * @param topicName the name of the topic
     * @return the topic entity
     * @throws ResourceNotFoundException if topic doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public VaradhiTopic getTopic(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
    }

    /**
     * Retrieves topic names for a project.
     *
     * @param projectName the name of the project
     * @return list of topic names
     * @throws ResourceNotFoundException if project doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<String> getTopicNames(String projectName) {
        String projectPrefix = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(TOPIC);
        return zkMetaStore.listChildren(znode).stream().filter(name -> name.startsWith(projectPrefix)).toList();
    }

    /**
     * Checks if a topic exists.
     *
     * @param topicName the name of the topic
     * @return true if topic exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkTopicExists(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing topic.
     *
     * @param topic the topic to update
     * @throws ResourceNotFoundException            if topic doesn't exist
     * @throws IllegalArgumentException             if topic update is invalid
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateTopic(VaradhiTopic topic) {
        ZNode znode = ZNode.ofTopic(topic.getName());
        zkMetaStore.updateTrackedZNodeWithData(znode, topic, ResourceType.TOPIC);
    }

    /**
     * Deletes a topic by its name.
     *
     * @param topicName the name of the topic to delete
     * @throws ResourceNotFoundException            if topic doesn't exist
     * @throws InvalidOperationForResourceException if topic has associated subscriptions
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteTopic(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        zkMetaStore.deleteTrackedZNode(znode, ResourceType.TOPIC);
    }
}