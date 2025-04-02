package com.flipkart.varadhi.db;

import com.flipkart.varadhi.spi.db.IamPolicy.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.org.OrgMetaStore;
import com.flipkart.varadhi.spi.db.project.ProjectMetaStore;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionMetaStore;
import com.flipkart.varadhi.spi.db.team.TeamMetaStore;
import com.flipkart.varadhi.spi.db.topic.TopicMetaStore;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public final class VaradhiMetaStore implements MetaStore {
    private final VaradhiMetaStoreImpl VaradhiOperationsImpl;
    private final ZKMetaStore zkMetaStore;

    /**
     * Constructs a new VaradhiMetaStore instance.
     *
     * <p>This constructor initializes the ZooKeeper-based metadata store and ensures
     * all required entity paths exist.
     *
     * @param zkMetaStore the ZooKeeper curator framework instance, must not be null
     * @throws IllegalArgumentException if zkCurator is null
     * @throws MetaStoreException       if initialization fails or required paths cannot be created
     */
    public VaradhiMetaStore(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
        VaradhiOperationsImpl = new VaradhiMetaStoreImpl(zkMetaStore);
    }

    @Override
    public OrgMetaStore orgOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public TeamMetaStore teamOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public ProjectMetaStore projectOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public TopicMetaStore topicOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public SubscriptionMetaStore subscriptionOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public IamPolicyMetaStore iamPolicyOperations() {
        return VaradhiOperationsImpl;
    }

    /**
     * @param listener
     * @return
     */
    @Override
    public boolean registerEventListener(MetaStoreEventListener listener) {
        return zkMetaStore.registerEventListener(listener);
    }
}
