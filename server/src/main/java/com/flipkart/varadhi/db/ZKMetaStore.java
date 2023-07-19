package com.flipkart.varadhi.db;

import com.flipkart.varadhi.exceptions.MetaStoreException;
import com.flipkart.varadhi.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.List;


@Slf4j
public class ZKMetaStore {

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
            throw new MetaStoreException(String.format("Failed to create entity %s.", resource));
        }
    }

    public <T> T get(String resourcePath, Class<T> clazz) {
        try {
            byte[] data = zkCurator.getData().forPath(resourcePath);
            return JsonMapper.jsonDeserialize(new String(data), clazz);
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Failed to get entity %s ", resourcePath));
        }
    }

    public boolean exists(String resourcePath) {
        try {
            return null != zkCurator.checkExists().forPath(resourcePath);
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Failed to check entity %s ", resourcePath));
        }
    }

    public void delete(String resourcePath) {
        try {
            zkCurator.delete().forPath(resourcePath);
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Failed to delete entity %s ", resourcePath));
        }
    }

    public List<String> list(String resourcePath) {
        try {
            if (!exists(resourcePath)) {
                throw new MetaStoreException(String.format("Resource Path %s does not exist", resourcePath));
            }
            return zkCurator.getChildren().forPath(resourcePath);
        } catch (Exception e) {
            throw new MetaStoreException(String.format("Failed to list entity on %s ", resourcePath));
        }
    }
}
