package com.flipkart.varadhi.db;

import com.flipkart.varadhi.exceptions.VaradhiException;
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
            throw new VaradhiException(e);
            log.error(String.format("Failed to create entity %s.", resource), e);
            throw new MetaStoreException(e);
        }
    }

    public <T> T get(String resourcePath, Class<T> clazz) {
        try {
            byte[] data = zkCurator.getData().forPath(resourcePath);
            return JsonMapper.jsonDeserialize(new String(data), clazz);
        } catch (Exception e) {
            throw new VaradhiException(e);
            log.error(String.format("Failed to get entity %s ", resourcePath), e);
            throw new MetaStoreException(e);
        }
    }

    public boolean exists(String resourcePath) {
        try {
            return null != zkCurator.checkExists().forPath(resourcePath);
        } catch (Exception e) {
            throw new VaradhiException(e);
            log.error(String.format("Failed to check entity %s ", resourcePath), e);
            throw new MetaStoreException(e);
        }
    }

    public void delete(String resourceKey) {
        String resourcePath = getResourcePath(resourceKey);
        try {
            zkCurator.delete().forPath(resourcePath);
        } catch (Exception e) {
            log.error(String.format("Failed to delete entity %s ", resourcePath), e);
            throw new MetaStoreException(e);
        }
    }

    public List<String> list(String resourceKey) {
        String resourcePath = getResourcePath(resourceKey);
        try {
            if (!exists(resourceKey)) {
                throw new MetaStoreException(String.format("Resource Path %s does not exist", resourceKey));
            }
            return zkCurator.getChildren().forPath(resourcePath);
        } catch (Exception e) {
            log.error(String.format("Failed to list entity on %s ", resourcePath), e);
            throw new MetaStoreException(e);
        }
    }

    private String getResourcePath(String resourceKey) {
        return String.format("%s/%s", BASE_PATH, resourceKey);
    }
}
