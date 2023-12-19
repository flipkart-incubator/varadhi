package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.spi.services.TopicService;
import lombok.extern.slf4j.Slf4j;


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
                    if (topicService.checkTopicExists(storageTopic)) {
                        log.warn("Specified StorageTopic({}:{}) already exists.", project.getName(), storageTopic.getName());
                    } else {
                        topicService.create(storageTopic, project);
                    }
                }
        );
        metaStore.createVaradhiTopic(varadhiTopic);
    }

    @Override
    public VaradhiTopic get(String topicName) {
        return metaStore.getVaradhiTopic(topicName);
    }

    @Override
    public void delete(VaradhiTopic varadhiTopic) {
        log.info("Deleting Varadhi topic {}", varadhiTopic.getName());
        /*TODO : delete namespace, tenant also if the only Topic in the namespace+tenant is deleted / cleanup independent of delete
         * check for existing subscriptions before deleting the topic
         */
        varadhiTopic.getInternalTopics().forEach((kind, internalTopic) ->
                {
                    StorageTopic storageTopic = internalTopic.getStorageTopic();
                    //do not delete if storage topic doesn't exist. Do add warn, if storage topic not found.
                    if(topicService.checkTopicExists(storageTopic)) {
                        topicService.delete(storageTopic);
                    } else {
                        log.warn("Specified StorageTopic({}) does not exist.", storageTopic.getName());
                    }
                }
        );
        metaStore.deleteVaradhiTopic(varadhiTopic.getName());
    }

    @Override
    public boolean checkTopicExists(VaradhiTopic topic) {
        //TODO : implementation
        return false;
    }

}
