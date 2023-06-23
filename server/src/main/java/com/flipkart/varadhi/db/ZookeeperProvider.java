package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.utils.YamlLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ZookeeperProvider implements MetaStoreProvider {


    //TODO::zk client under zkCurator needs to be closed.
    //TODO::check if zkCurator needs to be singleton.

    private final Map<Class<?>, MetaStore> metaStoreMap = new ConcurrentHashMap<>();
    private volatile boolean initialised = false;
    private ZKMetaStore zkMetaStore;

    public void init(MetaStoreOptions MetaStoreOptions) {
        if (!initialised) {
            synchronized (this) {
                if (!initialised) {
                    ZKMetaStoreConfig zkMetaStoreConfig =
                            YamlLoader.loadConfig(MetaStoreOptions.getConfigFile(), ZKMetaStoreConfig.class);
                    CuratorFramework zkCurator = getZkCurator(zkMetaStoreConfig.getZookeeperOptions());
                    this.zkMetaStore = new ZKMetaStore(zkCurator);
                    initialised = true;
                }
            }
        }
    }


    private CuratorFramework getZkCurator(ZookeeperOptions zkOptions) {
        //TODO:: Close on below
        // 1. Retry policy need to be configurable ?
        // 2. Need for ZKClientConfig while creating CuratorFramework.
        RetryPolicy retryPolicy = new RetryForever(200);
        CuratorFramework zkCurator = CuratorFrameworkFactory.builder()
                .connectString(zkOptions.getConnectUrl())
                .connectionTimeoutMs(zkOptions.getConnectTimeout())
                .sessionTimeoutMs(zkOptions.getSessionTimeout())
                .retryPolicy(retryPolicy)
                .build();
        zkCurator.start();
        return zkCurator;
    }

    public <T extends MetaStore<?>> T getMetaStore(Class<?> clazz) {
        if (!initialised) {
            throw new InvalidStateException("Zookeeper MetaStore is not yet initialised.");
        }
        MetaStore<T> metaStore = metaStoreMap.computeIfAbsent(clazz, z -> {
                    String clazzName = z.getName();
                    if (clazzName == VaradhiTopic.class.getName()) {
                        return new VaradhiTopicMetaStore(zkMetaStore);
                    } else if (clazzName == TopicResource.class.getName()) {
                        return new TopicResourceMetaStore(zkMetaStore);
                    } else {
                        String errMsg = String.format("Metastore not implemented for %s", clazzName);
                        log.error(errMsg);
                        throw new NotImplementedException(errMsg);
                    }
                }
        );
        return (T) metaStore;
    }

}


