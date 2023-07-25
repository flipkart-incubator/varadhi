package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.MetaStore;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
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
    public void create(VaradhiTopic varadhiTopic) {
        log.info("Creating Varadhi topic {}", varadhiTopic.getName());
        metaStore.createVaradhiTopic(varadhiTopic);
        varadhiTopic.getInternalTopics().forEach((kind, internalTopic) ->
                {
                    StorageTopic storageTopic = internalTopic.getStorageTopic();
                    topicService.create(storageTopic);
                }
        );
    }

    @Override
    public VaradhiTopic get(String topicName) {
        return metaStore.getVaradhiTopic(topicName);
    }
}
