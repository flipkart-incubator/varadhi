package com.flipkart.varadhi.db;

import com.flipkart.varadhi.spi.db.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.OrgMetaStore;
import com.flipkart.varadhi.spi.db.ProjectMetaStore;
import com.flipkart.varadhi.spi.db.SubscriptionMetaStore;
import com.flipkart.varadhi.spi.db.TeamMetaStore;
import com.flipkart.varadhi.spi.db.TopicMetaStore;

/**
 * Implementation of the metadata store for Varadhi using ZooKeeper as the backend.
 * This class provides operations for managing organizational entities
 * and their associated metadata.
 *
 * <p>The store maintains hierarchical data for:
 * <ul>
 *   <li>Organizations</li>
 *   <li>Teams</li>
 *   <li>Projects</li>
 *   <li>Topics</li>
 *   <li>Subscriptions</li>
 *   <li>IAM Policies</li>
 *   <li>Events</li>
 * </ul>
 */
public final class VaradhiMetaStore implements MetaStore, IamPolicyMetaStore.Provider {
    private final VaradhiMetaStoreImpl storeImpl;
    private final ZKMetaStore zkMetaStore;

    /**
     * @param zkMetaStore the ZooKeeper curator framework instance, must not be null
     * @throws IllegalArgumentException if zkCurator is null
     * @throws MetaStoreException       if initialization fails or required paths cannot be created
     */
    public VaradhiMetaStore(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
        storeImpl = new VaradhiMetaStoreImpl(zkMetaStore);
    }

    @Override
    public OrgMetaStore orgMetaStore() {
        return storeImpl;
    }

    @Override
    public TeamMetaStore teamMetaStore() {
        return storeImpl;
    }

    @Override
    public ProjectMetaStore projectMetaStore() {
        return storeImpl;
    }

    @Override
    public TopicMetaStore topicMetaStore() {
        return storeImpl;
    }

    @Override
    public SubscriptionMetaStore subscriptionMetaStore() {
        return storeImpl;
    }

    /**
     * @param listener
     * @return
     */
    @Override
    public boolean registerEventListener(MetaStoreEventListener listener) {
        return zkMetaStore.registerEventListener(listener);
    }

    /**
     * @return
     */
    @Override
    public IamPolicyMetaStore iamPolicyMetaStore() {
        return storeImpl;
    }
}
