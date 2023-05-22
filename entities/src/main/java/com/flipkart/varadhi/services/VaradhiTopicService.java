package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class VaradhiTopicService implements TopicService<VaradhiTopic> {

    private final StorageTopicServiceFactory<StorageTopic> topicServiceFactory;

    public VaradhiTopicService(StorageTopicServiceFactory<StorageTopic> serviceFactory) {
        topicServiceFactory = serviceFactory;
    }

    @Override
    public void create(VaradhiTopic varadhiTopic) {
        log.info("Creating Varadhi topic {}", varadhiTopic.getName());
        //TODO::Take care of persisting in meta store.
        varadhiTopic.getInternalTopics().forEach((kind, internalTopic) ->
                {
                    StorageTopic storageTopic = internalTopic.getStorageTopic();
                    topicServiceFactory.get().create(storageTopic);
                }
        );
    }
}
