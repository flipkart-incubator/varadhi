package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;


@Slf4j
public class ZKMetaStore  {

    private final CuratorFramework zkCurator;

    public ZKMetaStore(CuratorFramework zkCurator) {
        this.zkCurator = zkCurator;
    }


    public <T> String create(T resource, int version, String resourcePath) {
        try {
            String jsonData = JsonMapper.jsonSerialize(resource);
            String response = zkCurator.create().orSetData(version)
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(resourcePath, jsonData.getBytes());

            log.debug("Persisted Entity({}) in ZK: {}", resourcePath, response);
            return response;
        } catch (Exception e) {
            log.error(String.format("Failed to persist Entity(%s).", resourcePath), e);
            throw new VaradhiException(e);
        }
    }

    public <T> T get(String resourcePath, Class<T> clazz) {
        try {
            byte[] data = zkCurator.getData().forPath(resourcePath);
            return JsonMapper.jsonDeserialize(new String(data), clazz);
        } catch (Exception e) {
            log.error(String.format("Failed to get entity(%s).", resourcePath), e);
            //TODO::evaluate if VaradhiException is correct if it was refactored into independent plugin module.
            throw new VaradhiException(e);
        }
    }

    public boolean exists(String resourcePath) {
        try {
            return null != zkCurator.checkExists().forPath(resourcePath);
        } catch (Exception e) {
            log.error(String.format("Failed to check entity %s ", resourcePath), e);
            throw new VaradhiException(e);
        }
    }

}
