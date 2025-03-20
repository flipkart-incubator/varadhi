package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionOperations;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.SUBSCRIPTION;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;

public class SubscriptionOperationsImpl implements SubscriptionOperations {
    private final ZKMetaStore zkMetaStore;

    public SubscriptionOperationsImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
    }

    /**
     * Creates a new subscription.
     *
     * @param subscription the subscription to create
     * @throws IllegalArgumentException   if subscription is null or invalid
     * @throws DuplicateResourceException if subscription already exists
     * @throws ResourceNotFoundException  if associated project or topic doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        zkMetaStore.createTrackedZNodeWithData(znode, subscription, ResourceType.SUBSCRIPTION);
    }

    /**
     * Retrieves a subscription by its name.
     *
     * @param subscriptionName the name of the subscription
     * @return the subscription entity
     * @throws ResourceNotFoundException if subscription doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public VaradhiSubscription getSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiSubscription.class);
    }

    /**
     * Retrieves all subscription names across all projects.
     *
     * @return list of all subscription names
     * @throws MetaStoreException if there's an error during retrieval
     */
    @Override
    public List<String> getAllSubscriptionNames() {
        ZNode znode = ZNode.ofEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode).stream().toList();
    }

    /**
     * Retrieves subscription names for a project.
     *
     * @param projectName the name of the project
     * @return list of subscription names
     * @throws ResourceNotFoundException if project doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<String> getSubscriptionNames(String projectName) {
        String projectPrefix = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode).stream().filter(name -> name.contains(projectPrefix)).toList();
    }

    /**
     * Checks if a subscription exists.
     *
     * @param subscriptionName the name of the subscription
     * @return true if subscription exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkSubscriptionExists(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing subscription.
     *
     * @param subscription the subscription to update
     * @throws ResourceNotFoundException            if subscription doesn't exist
     * @throws IllegalArgumentException             if subscription update is invalid
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        zkMetaStore.updateTrackedZNodeWithData(znode, subscription, ResourceType.SUBSCRIPTION);
    }

    /**
     * Deletes a subscription by its name.
     *
     * @param subscriptionName the name of the subscription to delete
     * @throws ResourceNotFoundException if subscription doesn't exist
     * @throws MetaStoreException        if there's an error during deletion
     */
    @Override
    public void deleteSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        zkMetaStore.deleteTrackedZNode(znode, ResourceType.SUBSCRIPTION);
    }
}