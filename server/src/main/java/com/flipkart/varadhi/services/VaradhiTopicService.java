package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

// TODO::This need to move to server.services
@Slf4j
public class VaradhiTopicService {

    private final StorageTopicService<StorageTopic> topicService;
    private final MetaStore metaStore;

    public VaradhiTopicService(
            StorageTopicService<StorageTopic> storageTopicService,
            MetaStore metaStore
    ) {
        this.topicService = storageTopicService;
        this.metaStore = metaStore;
    }

    public void create(VaradhiTopic varadhiTopic, Project project) {
        log.info("Creating Varadhi topic {}", varadhiTopic.getName());
        // StorageTopicService.create() to ensure if pre-existing topic can be re-used.
        // i.e. topic creation at storage level need to be idempotent.
        varadhiTopic.getInternalTopics().forEach((region, internalTopic) -> internalTopic.getActiveTopics()
                .forEach(storageTopic -> topicService.create(storageTopic, project)));
        metaStore.createTopic(varadhiTopic);
    }

    public VaradhiTopic get(String topicName) {
        return metaStore.getTopic(topicName);
    }

    public void delete(String varadhiTopicName) {
        log.info("Deleting Varadhi topic {}", varadhiTopicName);
        /* TODO : delete namespace, tenant also if the only Topic in the namespace+tenant is deleted / cleanup independent of delete
         */
        VaradhiTopic varadhiTopic = metaStore.getTopic(varadhiTopicName);
        String projectName = varadhiTopic.getProjectName();
        Project project = metaStore.getProject(projectName);
        validateDelete(varadhiTopicName);
        varadhiTopic.getInternalTopics().forEach((region, internalTopic) -> internalTopic.getActiveTopics()
                .forEach(storageTopic -> topicService.delete(storageTopic.getName(), project)));
        metaStore.deleteTopic(varadhiTopic.getName());
    }

    private void validateDelete(String varadhiTopicName) {
        // TODO: optimize this flow, currently it scans all subscriptions across all projects
        List<String> subscriptionNames = metaStore.getAllSubscriptionNames();
        subscriptionNames.forEach(subscriptionName -> {
            VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);
            if (subscription.getTopic().equals(varadhiTopicName)) {
                throw new InvalidOperationForResourceException(
                        "Cannot delete topic as it is being used by subscription " + subscriptionName);
            }
        });
    }

    public boolean exists(String topicName) {
        return metaStore.checkTopicExists(topicName);
    }

    public List<String> getVaradhiTopics(String projectName) {
        return metaStore.getTopicNames(projectName);
    }
}
