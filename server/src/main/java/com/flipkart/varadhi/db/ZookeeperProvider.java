package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.utils.YamlLoader;
import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.OpStore;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provider implementation for ZooKeeper-based metadata storage.
 * This class manages the lifecycle of various store implementations and ensures
 * thread-safe initialization of the ZooKeeper client and related resources.
 *
 * <p>The provider implements a singleton pattern for store instances and manages:
 * <ul>
 *     <li>Metadata Store - For general metadata operations</li>
 *     <li>Operation Store - For managing operational tasks</li>
 *     <li>Assignment Store - For handling resource assignments</li>
 * </ul>
 *
 * <p>Thread-safety is ensured through atomic operations and proper resource management.
 *
 * @see MetaStoreProvider
 * @see CuratorFramework
 * @see VaradhiMetaStore
 * @see OpStoreImpl
 * @see AssignmentStoreImpl
 */
@Slf4j
public class ZookeeperProvider implements MetaStoreProvider {
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private CuratorFramework zkCurator;
    private ZKMetaStore zkMetaStore;
    private VaradhiMetaStore varadhiMetaStore;
    private OpStoreImpl opStore;
    private AssignmentStoreImpl assignmentStore;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(MetaStoreOptions metaStoreOptions) {
        if (initialized.compareAndSet(false, true)) {
            try {
                ZKMetaStoreConfig zkMetaStoreConfig = YamlLoader.loadConfig(
                    metaStoreOptions.getConfigFile(),
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

    /**
     * Initializes all store implementations with the configured ZooKeeper curator.
     * This method should only be called once during initialization.
     */
    private void initializeStores() {
        zkMetaStore = new ZKMetaStore(zkCurator);
        varadhiMetaStore = new VaradhiMetaStore(zkMetaStore);
        opStore = new OpStoreImpl(zkMetaStore);
        assignmentStore = new AssignmentStoreImpl(zkMetaStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaStore getMetaStore() {
        checkInitialized();
        return varadhiMetaStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OpStore getOpStore() {
        checkInitialized();
        return opStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AssignmentStore getAssignmentStore() {
        checkInitialized();
        return assignmentStore;
    }

    /**
     * Verifies that the provider has been properly initialized.
     *
     * @throws IllegalStateException if the provider is not initialized
     */
    private void checkInitialized() {
        if (!initialized.get()) {
            throw new IllegalStateException("ZookeeperProvider is not initialized");
        }
    }

    /**
     * Closes the ZooKeeper provider and releases all resources.
     * This method is idempotent and can be called multiple times safely.
     * Any errors during closure are logged but not propagated.
     */
    @Override
    public void close() {
        if (initialized.get() && zkCurator != null) {
            try {
                if (zkMetaStore != null) {
                    zkMetaStore.close();
                }
                zkCurator.close();
            } catch (Exception e) {
                log.error("Error closing ZooKeeper curator", e);
            } finally {
                initialized.set(false);
            }
        }
    }
}
