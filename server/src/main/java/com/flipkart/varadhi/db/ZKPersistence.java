package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.KeyProvider;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.Collections;
import java.util.List;


/*
1. Unique key/path per object, organised in hierarchy on searchable attributes (hierarchical e.g. org/team/project).
This can be enforced by some interface
e.g.
  /varadhi/entities/topicresource/fk/team1/topic1
  /varadhi/entities/varadhitopic/fk/team1/topic1
  /varadhi/entities/subscriptions/fk/team1/sub1
        -- how to get all subs for a topic ? -- maintain sub list under topic as children

2. data should be versioned to prevent stale writes. -- interface/extends.
3. object should be (de)serializable. -- interface/extends.
4. data size governed by ZK constraints.
5. no history being maintained for entity.
 */
@Slf4j
public class ZKPersistence<T extends KeyProvider> implements Persistence<T> {
    private static final String BASE_PATH = "/varadhi/entities";
    private static final int INITIAL_VERSION = 0;

    //TODO::zk client under zkCurator needs to be closed.
    //TODO::check if zkCurator needs to be singleton.
    private final CuratorFramework zkCurator;

    public ZKPersistence(CuratorFramework zkCurator) {
        this.zkCurator = zkCurator;
    }


    public void create(T resource) {
        String resourcePath = getResourcePath(resource.uniqueKeyPath());
        String jsonData = JsonMapper.jsonSerialize(resource);
        try {
            String response = zkCurator.create().orSetData(INITIAL_VERSION)
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(resourcePath, jsonData.getBytes());

            log.debug("zkCurator response:{}", response);
        } catch (Exception e) {
            log.error(String.format("Failed to create entity %s.", resource), e);
            throw new VaradhiException(e);
        }
    }

    public T get(String resourceKey, Class<T> clazz) {
        String resourcePath = getResourcePath(resourceKey);
        try {
            byte[] data = zkCurator.getData().forPath(resourcePath);
            return JsonMapper.jsonDeserialize(new String(data), clazz);
        } catch (Exception e) {
            log.error(String.format("Failed to get entity %s ", resourcePath), e);
            throw new VaradhiException(e);
        }
    }

    public boolean exists(String resourceKey) {
        String resourcePath = getResourcePath(resourceKey);
        try {
            return null != zkCurator.checkExists().forPath(resourcePath);
        } catch (Exception e) {
            log.error(String.format("Failed to check entity %s ", resourcePath), e);
            throw new VaradhiException(e);
        }
    }

    public void delete(String resourceKey) {
        String resourcePath = getResourcePath(resourceKey);
        try {
            zkCurator.delete().forPath(resourcePath);
        } catch (Exception e) {
            log.error(String.format("Failed to delete entity %s ", resourcePath), e);
            throw new VaradhiException(e);
        }
    }

    public List<String> list(String resourceKey) {
        String resourcePath = getResourcePath(resourceKey);
        try {
            if (!exists(resourceKey)) return Collections.emptyList();
            return zkCurator.getChildren().forPath(resourcePath);
        } catch (Exception e) {
            log.error(String.format("Failed to list entity on %s ", resourcePath), e);
            throw new VaradhiException(e);
        }
    }

    private String getResourcePath(String resourceKey) {
        return String.format("%s%s", BASE_PATH, resourceKey);
    }
}
