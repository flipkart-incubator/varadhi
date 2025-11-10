package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.*;

public interface StorageTopicFactory<T extends StorageTopic> {

    // TODO:: This will change further to take care of producing and replicating regions as well.

    /**
     * Get StorageTopic instance
     * 
     * @param id           Storage Topic id. This id is useful to identify specific storage topic among the list of storage topics present in the Varadhi Topic.
     * @param topicName    Name of the storage topic. It is a globally unique fqn understood by the underlying messaging system. Messaging stack can take a 
     *                      dependency on this to create either global or regional topic as required.
     * @param project      Project to which this topic belongs. TODO: storage topic lives under a varadhi topic, not directly under the project. So this param
    *                           may need to be changed to Varadhi Topic instead.
     * @param capacity     Topic capacity policy.
     * @param queueCategory Internal queue category. This represents the usecase for which this topic is being created.
     * @return StorageTopic instance
     */
    T getTopic(
        int id,
        String topicName,
        Project project,
        TopicCapacityPolicy capacity,
        InternalQueueCategory queueCategory
    );
}
