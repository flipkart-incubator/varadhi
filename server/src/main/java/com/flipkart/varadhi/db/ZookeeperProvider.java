package com.flipkart.varadhi.db;

import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import com.flipkart.varadhi.utils.YamlLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

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
                    CuratorFramework zkCurator =
                            CuratorFrameworkCreator.create(zkMetaStoreConfig.getZookeeperOptions());
                    this.varadhiMetaStore = new VaradhiMetaStore(zkCurator);
                    initialised = true;
                }
            }
        }
    }

    public MetaStore getMetaStore() {
        if (!initialised) {
            throw new IllegalStateException("Zookeeper MetaStore is not yet initialised.");
        }
        return this.varadhiMetaStore;
    }
}


