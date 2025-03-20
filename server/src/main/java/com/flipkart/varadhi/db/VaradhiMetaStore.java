package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.spi.db.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.org.OrgFilterOperations;
import com.flipkart.varadhi.spi.db.org.OrgOperations;
import com.flipkart.varadhi.spi.db.project.ProjectOperations;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionOperations;
import com.flipkart.varadhi.spi.db.team.TeamOperations;
import com.flipkart.varadhi.spi.db.topic.TopicOperations;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.varadhi.db.ZNode.*;

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
public final class VaradhiMetaStore implements MetaStore, IamPolicyMetaStore {
    private final ZKMetaStore zkMetaStore;
    private final OrgOperations orgOperations;
    private final TeamOperations teamOperations;
    private final ProjectOperations projectOperations;
    private final TopicOperations topicOperations;
    private final SubscriptionOperations subscriptionOperations;
    private final OrgFilterOperations orgLevelFilters;

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
        this.orgOperations = new OrgOperationsImpl(zkMetaStore);
        this.teamOperations = new TeamOperationsImpl(zkMetaStore);
        this.projectOperations = new ProjectOperationsImpl(zkMetaStore);
        this.topicOperations = new TopicOperationsImpl(zkMetaStore);
        this.subscriptionOperations = new SubscriptionOperationsImpl(zkMetaStore);
        this.orgLevelFilters = new OrgFilterOperationsImpl(zkMetaStore);
        ensureEntityTypePathExists();
    }

    /**
     * Ensures that all required entity type paths exist in ZooKeeper.
     * Creates missing paths if necessary.
     *
     * @throws MetaStoreException if path creation fails
     */
    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.ofEntityType(ORG));
        zkMetaStore.createZNode(ZNode.ofEntityType(TEAM));
        zkMetaStore.createZNode(ZNode.ofEntityType(PROJECT));
        zkMetaStore.createZNode(ZNode.ofEntityType(TOPIC));
        zkMetaStore.createZNode(ZNode.ofEntityType(SUBSCRIPTION));
        zkMetaStore.createZNode(ZNode.ofEntityType(IAM_POLICY));
        zkMetaStore.createZNode(ZNode.ofEntityType(EVENT));
    }

    @Override
    public OrgOperations orgOperations() {
        return orgOperations;
    }

    @Override
    public OrgFilterOperations orgLevelFilters() {
        return orgLevelFilters;
    }

    @Override
    public TeamOperations teamOperations() {
        return teamOperations;
    }

    @Override
    public ProjectOperations projectOperations() {
        return projectOperations;
    }

    @Override
    public TopicOperations topicOperations() {
        return topicOperations;
    }

    @Override
    public SubscriptionOperations subscriptionOperations() {
        return subscriptionOperations;
    }

    /**
     * Registers an event listener for metadata store events.
     *
     * @param listener the event listener to register
     * @return true if registration was successful, false otherwise
     * @throws IllegalArgumentException if listener is null
     * @throws MetaStoreException       if there's an error during registration
     */
    @Override
    public boolean registerEventListener(MetaStoreEventListener listener) {
        return zkMetaStore.registerEventListener(listener);
    }

    /**
     * Creates a new IAM policy record in the metadata store.
     *
     * @param iamPolicyRecord the IAM policy record to create
     * @throws IllegalArgumentException   if iamPolicyRecord is null or invalid
     * @throws DuplicateResourceException if a policy with the same name already exists
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        ZNode znode = ZNode.ofIamPolicy(iamPolicyRecord.getName());
        zkMetaStore.createZNodeWithData(znode, iamPolicyRecord);
    }

    /**
     * Retrieves an IAM policy record by its resource ID.
     *
     * @param authResourceId the unique identifier of the IAM policy
     * @return the IAM policy record
     * @throws IllegalArgumentException  if authResourceId is null or empty
     * @throws ResourceNotFoundException if the policy doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public IamPolicyRecord getIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        return zkMetaStore.getZNodeDataAsPojo(znode, IamPolicyRecord.class);
    }

    /**
     * Checks if an IAM policy record exists for the given resource ID.
     *
     * @param authResourceId the unique identifier of the IAM policy
     * @return true if the policy exists, false otherwise
     * @throws IllegalArgumentException if authResourceId is null or empty
     * @throws MetaStoreException       if there's an error checking existence
     */
    @Override
    public boolean isIamPolicyRecordPresent(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing IAM policy record.
     *
     * @param iamPolicyRecord the IAM policy record to update
     * @throws IllegalArgumentException             if iamPolicyRecord is null or invalid
     * @throws ResourceNotFoundException            if the policy doesn't exist
     * @throws InvalidOperationForResourceException if the update operation is invalid
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        ZNode znode = ZNode.ofIamPolicy(iamPolicyRecord.getName());
        zkMetaStore.updateZNodeWithData(znode, iamPolicyRecord);
    }

    /**
     * Deletes an IAM policy record.
     *
     * @param authResourceId the unique identifier of the IAM policy to delete
     * @throws IllegalArgumentException  if authResourceId is null or empty
     * @throws ResourceNotFoundException if the policy doesn't exist
     * @throws MetaStoreException        if there's an error during deletion
     */
    @Override
    public void deleteIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        zkMetaStore.deleteZNode(znode);
    }
}
