package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.TopicResource;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TopicResourceMetaStore extends BaseMetaStore implements MetaStore<TopicResource> {
    private static final String TOPIC_RESOURCE_NAME = "TopicResource";

    public TopicResourceMetaStore(ZKMetaStore zkMetaStore) {
        super(zkMetaStore);
    }


    @Override
    public TopicResource get(String projectName, String topicName) {
        String resourcePath = getResourcePath(projectName, topicName);
        return this.zkMetaStore.get(resourcePath, TopicResource.class);
    }

    @Override
    public TopicResource create(TopicResource topicResource) {
        String resourcePath = getResourcePath(topicResource.getProject(), topicResource.getName());
        topicResource.setVersion(INITIAL_VERSION);
        this.zkMetaStore.create(topicResource, topicResource.getVersion(), resourcePath);
        return topicResource;
    }

    @Override
    public boolean exists(String projectName, String topicName) {
        String resourcePath = getResourcePath(projectName, topicName);
        return this.zkMetaStore.exists(resourcePath);
    }

    private String getResourcePath(String projectName, String topicName) {
        return constructPath(TOPIC_RESOURCE_NAME, projectName, topicName);
    }
}
