package com.flipkart.varadhi.db;

import com.flipkart.varadhi.spi.db.IamPolicy.IamPolicyOperations;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.org.OrgOperations;
import com.flipkart.varadhi.spi.db.project.ProjectOperations;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionOperations;
import com.flipkart.varadhi.spi.db.team.TeamOperations;
import com.flipkart.varadhi.spi.db.topic.TopicOperations;
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
    private final VaradhiOperationsImpl VaradhiOperationsImpl;

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
        VaradhiOperationsImpl = new VaradhiOperationsImpl(zkMetaStore);
    }

    @Override
    public OrgOperations orgOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public TeamOperations teamOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public ProjectOperations projectOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public TopicOperations topicOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public SubscriptionOperations subscriptionOperations() {
        return VaradhiOperationsImpl;
    }

    @Override
    public IamPolicyOperations iamPolicyOperations() {
        return VaradhiOperationsImpl;
    }

    /**
     * @param listener
     * @return
     */
    @Override
    public boolean registerEventListener(MetaStoreEventListener listener) {
        return false;
    }
}
