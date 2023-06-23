package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.KeyProvider;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.utils.YamlLoader;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

public class ZookeeperProvider implements MetaStoreProvider {

    private volatile boolean initialised = false;
    private CuratorFramework zkCurator;

    public void init(MetaStoreOptions MetaStoreOptions) {
        if (!initialised) {
            synchronized (this) {
                if (!initialised) {
                    ZKMetaStoreConfig zkMetaStoreConfig =
                            YamlLoader.loadConfig(MetaStoreOptions.getConfigFile(), ZKMetaStoreConfig.class);
                    zkCurator = getZkCurator(zkMetaStoreConfig.getZookeeperOptions());
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
        zkCurator = CuratorFrameworkFactory.builder()
                .connectString(zkOptions.getConnectUrl())
                .connectionTimeoutMs(zkOptions.getConnectTimeout())
                .sessionTimeoutMs(zkOptions.getSessionTimeout())
                .retryPolicy(retryPolicy)
                .build();
        zkCurator.start();
        return zkCurator;
    }

    public <T extends KeyProvider> MetaStore<T> getMetaStore() {
        if (!initialised) {
            throw new InvalidStateException("ZookeeperProvider is not yet initialised.");
        }
        return new ZKMetaStore<>(this.zkCurator);
    }
}


