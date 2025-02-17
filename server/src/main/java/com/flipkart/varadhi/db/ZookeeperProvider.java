package com.flipkart.varadhi.db;

import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.EventStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.OpStore;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import com.flipkart.varadhi.utils.YamlLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provider for Zookeeper-based metadata storage implementations.
 * <p>
 * This class manages the lifecycle of various store implementations and ensures
 * thread-safe initialization of the Zookeeper client and related resources.
 *
 * @see MetaStoreProvider
 * @see CuratorFramework
 */
@Slf4j
public class ZookeeperProvider implements MetaStoreProvider {
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private CuratorFramework zkCurator;
    private VaradhiMetaStore varadhiMetaStore;
    private OpStoreImpl opStore;
    private AssignmentStoreImpl assignmentStore;
    private EventStoreImpl eventStore;

    @Override
    public void init(MetaStoreOptions metaStoreOptions) {
        if (initialized.compareAndSet(false, true)) {
            try {
                ZKMetaStoreConfig zkMetaStoreConfig = YamlLoader.loadConfig(
                    metaStoreOptions.configFile(),
                    ZKMetaStoreConfig.class
                );
                zkCurator = CuratorFrameworkCreator.create(zkMetaStoreConfig.getZookeeperOptions());
                initializeStores();
            } catch (Exception e) {
                initialized.set(false);
                throw new IllegalStateException("Failed to initialize ZookeeperProvider", e);
            }
        }
    }

    private void initializeStores() {
        varadhiMetaStore = new VaradhiMetaStore(zkCurator);
        opStore = new OpStoreImpl(zkCurator);
        assignmentStore = new AssignmentStoreImpl(zkCurator);
        eventStore = new EventStoreImpl(zkCurator);
    }

    @Override
    public MetaStore getMetaStore() {
        checkInitialized();
        return varadhiMetaStore;
    }

    @Override
    public OpStore getOpStore() {
        checkInitialized();
        return opStore;
    }

    @Override
    public AssignmentStore getAssignmentStore() {
        checkInitialized();
        return assignmentStore;
    }

    @Override
    public EventStore getEventStore() {
        checkInitialized();
        return eventStore;
    }

    private void checkInitialized() {
        if (!initialized.get()) {
            throw new IllegalStateException("ZookeeperProvider is not initialized");
        }
    }

    @Override
    public void close() {
        if (initialized.get() && zkCurator != null) {
            try {
                zkCurator.close();
            } catch (Exception e) {
                log.error("Error closing ZooKeeper curator", e);
            } finally {
                initialized.set(false);
            }
        }
    }
}
