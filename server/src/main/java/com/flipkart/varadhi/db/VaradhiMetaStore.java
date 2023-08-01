package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;


@Slf4j
public class VaradhiMetaStore implements MetaStore {
    private final ZKMetaStore zkMetaStore;

    public VaradhiMetaStore(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
    }

    @Override
    public TopicResource createTopicResource(TopicResource resource) {
        String resourcePath = ZKPathUtils.getTopicResourcePath(resource.getProject(), resource.getName());
        this.zkMetaStore.create(resource, resource.getVersion(), resourcePath);
        return resource;
    }

    @Override
    public boolean checkTopicResourceExists(String projectName, String topicName) {
        String resourcePath = ZKPathUtils.getTopicResourcePath(projectName, topicName);
        return this.zkMetaStore.exists(resourcePath);
    }

    @Override
    public TopicResource getTopicResource(String projectName, String resourceName) {
        String resourcePath = ZKPathUtils.getTopicResourcePath(projectName, resourceName);
        return this.zkMetaStore.get(resourcePath, TopicResource.class);
    }

    @Override
    public VaradhiTopic createVaradhiTopic(VaradhiTopic varadhiTopic) {
        String resourcePath = ZKPathUtils.getVaradhiTopicPath(varadhiTopic.getName());
        this.zkMetaStore.create(varadhiTopic, varadhiTopic.getVersion(), resourcePath);
        return varadhiTopic;
    }

    @Override
    public boolean checkVaradhiTopicExists(String varadhiTopicName) {
        String resourcePath = ZKPathUtils.getVaradhiTopicPath(varadhiTopicName);
        return this.zkMetaStore.exists(resourcePath);
    }

    @Override
    public VaradhiTopic getVaradhiTopic(String topicName) {
        String resourcePath = ZKPathUtils.getVaradhiTopicPath(topicName);
        return this.zkMetaStore.get(resourcePath, VaradhiTopic.class);
    }

    @Override
    public void deleteVaradhiTopic(String topicName) {
        String resourcePath = ZKPathUtils.getVaradhiTopicPath(topicName);
        this.zkMetaStore.delete(resourcePath);
    }

    @Override
    public void deleteTopicResource(String projectName, String resourceName) {
        String resourcePath = ZKPathUtils.getTopicResourcePath(projectName, resourceName);
        this.zkMetaStore.delete(resourcePath);
    }
}
