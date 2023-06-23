package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.VaradhiTopic;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class VaradhiTopicMetaStore extends BaseMetaStore implements MetaStore<VaradhiTopic>{

    private static final String VARADHI_TOPIC_NAME = "VaradhiTopic";

    public VaradhiTopicMetaStore(ZKMetaStore zkMetaStore) {
        super(zkMetaStore);
    }

    @Override
    public VaradhiTopic get(String parent, String topicName) {
        String resourcePath  = getResourcePath(topicName);
        return this.zkMetaStore.get(resourcePath, VaradhiTopic.class);
    }

    @Override
    public VaradhiTopic create(VaradhiTopic varadhiTopic) {
        String resourcePath = getResourcePath(varadhiTopic.getName());
        //TODO::fix VaradhiTopic to have a version.
//        varadhiTopic.setVersion(INITIAL_VERSION);
        this.zkMetaStore.create(varadhiTopic, INITIAL_VERSION, resourcePath);
        return varadhiTopic;
    }

    @Override
    public boolean exists(String parent, String topicName) {
        String resourcePath = getResourcePath(topicName);
        return this.zkMetaStore.exists(resourcePath);
    }

    //VaradhiTopic(s) are globally unique w.r.to Naming convention.
    //They are internal topic, and name is system generated which ensures same.
    private String getResourcePath(String topicName) {
        return constructPath(VARADHI_TOPIC_NAME, topicName);
    }

}
