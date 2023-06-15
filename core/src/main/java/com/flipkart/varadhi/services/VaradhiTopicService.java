package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.Persistence;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class VaradhiTopicService implements TopicService<VaradhiTopic> {

    private final StorageTopicServiceFactory<StorageTopic> topicServiceFactory;
    private final Persistence<VaradhiTopic> topicPersistence;

    public VaradhiTopicService(
            StorageTopicServiceFactory<StorageTopic> serviceFactory,
            Persistence<VaradhiTopic> topicPersistence
    ) {
        this.topicServiceFactory = serviceFactory;
        this.topicPersistence = topicPersistence;
    }

    @Override
    public void create(VaradhiTopic varadhiTopic) {
        log.info("Creating Varadhi topic {}", varadhiTopic.getName());
        topicPersistence.create(varadhiTopic);
        varadhiTopic.getInternalTopics().forEach((kind, internalTopic) ->
                {
                    StorageTopic storageTopic = internalTopic.getStorageTopic();
                    topicServiceFactory.get().create(storageTopic);
                }
        );
    }
}
