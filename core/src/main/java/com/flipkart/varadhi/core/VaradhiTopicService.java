package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.spi.services.TopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.flipkart.varadhi.entities.MetaStoreEntity.NAME_SEPARATOR_REGEX;


@Slf4j
public class VaradhiTopicService implements TopicService<VaradhiTopic> {

    private final StorageTopicService<StorageTopic> topicService;
    private final MetaStore metaStore;

    public VaradhiTopicService(
            StorageTopicService<StorageTopic> serviceFactory,
            MetaStore metaStore
    ) {
        this.topicService = serviceFactory;
        this.metaStore = metaStore;
    }

    @Override
    public void create(VaradhiTopic varadhiTopic, Project project) {
        log.info("Creating Varadhi topic {}", varadhiTopic.getName());
        varadhiTopic.getInternalTopics().forEach((kind, internalTopic) ->
                {
                    StorageTopic storageTopic = internalTopic.getStorageTopic();
                    if (topicService.checkTopicExists(storageTopic.getName())) {
                        log.warn("Specified StorageTopic({}:{}) already exists.", project.getName(), storageTopic.getName());
                    } else {
                        topicService.create(storageTopic, project);
                    }
                }
        );
        metaStore.createTopic(varadhiTopic);
    }

    @Override
    public VaradhiTopic get(String topicName) {
        return metaStore.getTopic(topicName);
    }

    @Override
    public void delete(String varadhiTopicName) {
        log.info("Deleting Varadhi topic {}", varadhiTopicName);
        /*TODO : delete namespace, tenant also if the only Topic in the namespace+tenant is deleted / cleanup independent of delete
         * check for existing subscriptions before deleting the topic
         */
        VaradhiTopic varadhiTopic = metaStore.getTopic(varadhiTopicName);
        validateDelete(varadhiTopicName);
        varadhiTopic.getInternalTopics().forEach((kind, internalTopic) ->
                {
                    StorageTopic storageTopic = internalTopic.getStorageTopic();
                    if(topicService.checkTopicExists(storageTopic.getName())) {
                        topicService.delete(storageTopic.getName());
                    } else {
                        log.warn("Specified StorageTopic({}) does not exist.", storageTopic.getName());
                    }
                }
        );
        metaStore.deleteTopic(varadhiTopic.getName());
    }

    private void validateDelete(String varadhiTopicName) {
        String project = varadhiTopicName.split(NAME_SEPARATOR_REGEX)[0];
        List<String> subscriptionNames = metaStore.getSubscriptionNames(project);
        subscriptionNames.forEach(subscriptionName -> {
            VaradhiSubscription subscription = metaStore.getSubscription(subscriptionName);
            if (subscription.getTopic().equals(varadhiTopicName)) {
                throw new IllegalArgumentException(
                        "Cannot delete topic as it is being used by subscription " + subscriptionName);
            }
        });
    }

    @Override
    public boolean checkTopicExists(String topicName) {
        return metaStore.checkTopicExists(topicName);
    }

    public List<String> getVaradhiTopics(String projectName) {
        return metaStore.getTopicNames(projectName);
    }
}
