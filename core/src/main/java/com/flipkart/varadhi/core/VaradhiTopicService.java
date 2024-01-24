package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
public class VaradhiTopicService implements TopicService<VaradhiTopic> {

    private final StorageTopicService topicService;
    private final MetaStore metaStore;

    public VaradhiTopicService(
            StorageTopicService serviceFactory,
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
                    if (topicService.exists(storageTopic.getName())) {
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
    public void delete(String varadhiTopicName) {
        log.info("Deleting Varadhi topic {}", varadhiTopicName);
        /*TODO : delete namespace, tenant also if the only Topic in the namespace+tenant is deleted / cleanup independent of delete
         * check for existing subscriptions before deleting the topic
         */
        VaradhiTopic varadhiTopic = metaStore.getVaradhiTopic(varadhiTopicName);
        varadhiTopic.getInternalTopics().forEach((kind, internalTopic) ->
                {
                    StorageTopic storageTopic = internalTopic.getStorageTopic();
                    if (topicService.exists(storageTopic.getName())) {
                        topicService.delete(storageTopic.getName());
                    } else {
                        log.warn("Specified StorageTopic({}) does not exist.", storageTopic.getName());
                    }
                }
        );
        metaStore.deleteVaradhiTopic(varadhiTopic.getName());
    }

    @Override
    public boolean exists(String topicName) {
        return metaStore.checkVaradhiTopicExists(topicName);
    }

    public List<String> getVaradhiTopics(String projectName) {
        return metaStore.getVaradhiTopicNames(projectName);
    }
}
