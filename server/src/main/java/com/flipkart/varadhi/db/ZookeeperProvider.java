package com.flipkart.varadhi.db;

import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.utils.YamlLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

@Slf4j
public class ZookeeperProvider implements MetaStoreProvider {

    //TODO::zk client under zkCurator needs to be closed.

    private volatile boolean initialised = false;

    private VaradhiMetaStore varadhiMetaStore;

    public void init(MetaStoreOptions MetaStoreOptions) {
        if (!initialised) {
            synchronized (this) {
                if (!initialised) {
                    ZKMetaStoreConfig zkMetaStoreConfig =
                            YamlLoader.loadConfig(MetaStoreOptions.getConfigFile(), ZKMetaStoreConfig.class);
                    CuratorFramework zkCurator = getZkCurator(zkMetaStoreConfig.getZookeeperOptions());
                    this.varadhiMetaStore = new VaradhiMetaStore(zkCurator);
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

    public MetaStore getMetaStore() {
        if (!initialised) {
            throw new IllegalStateException("Zookeeper MetaStore is not yet initialised.");
        }
        return this.varadhiMetaStore;
    }

}


