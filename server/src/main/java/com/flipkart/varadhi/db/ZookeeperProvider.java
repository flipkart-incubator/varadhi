package com.flipkart.varadhi.db;

import com.flipkart.varadhi.spi.db.*;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import com.flipkart.varadhi.utils.YamlLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class ZookeeperProvider implements MetaStoreProvider {

    //TODO::zk client under zkCurator needs to be closed.

    private volatile boolean initialised = false;

    private VaradhiMetaStore varadhiMetaStore;
    private OpStoreImpl opStore;
    private AssignmentStoreImpl assignmentStore;

    public void init(MetaStoreOptions MetaStoreOptions) {
        if (!initialised) {
            synchronized (this) {
                if (!initialised) {
                    ZKMetaStoreConfig zkMetaStoreConfig =
                            YamlLoader.loadConfig(MetaStoreOptions.getConfigFile(), ZKMetaStoreConfig.class);
                    CuratorFramework zkCurator =
                            CuratorFrameworkCreator.create(zkMetaStoreConfig.getZookeeperOptions());
                    varadhiMetaStore = new VaradhiMetaStore(zkCurator);
                    opStore = new OpStoreImpl(zkCurator);
                    assignmentStore = new AssignmentStoreImpl(zkCurator);
                    initialised = true;
                }
            }
        }
    }

    @Override
    public MetaStore getMetaStore() {
        if (!initialised) {
            throw new IllegalStateException("Zookeeper MetaStore is not yet initialised.");
        }
        return varadhiMetaStore;
    }

    @Override
    public OpStore getOpStore() {
        if (!initialised) {
            throw new IllegalStateException("Zookeeper MetaStore is not yet initialised.");
        }
        return opStore;
    }

    @Override
    public AssignmentStore getAssignmentStore() {
        if (!initialised) {
            throw new IllegalStateException("Zookeeper MetaStore is not yet initialised.");
        }
        return assignmentStore;
    }
}


