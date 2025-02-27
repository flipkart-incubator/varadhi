package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.OpStore;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.SHARD_OP;
import static com.flipkart.varadhi.db.ZNode.SUB_OP;

/**
 * Implementation of the Operation Store using ZooKeeper as the underlying storage.
 * This class manages subscription and shard operations persistence.
 */
public class OpStoreImpl implements OpStore {
    private final ZKMetaStore zkMetaStore;

    /**
     * Constructs a new OpStoreImpl with the given ZooKeeper curator.
     *
     * @param zkCurator The ZooKeeper curator framework instance
     * @throws IllegalArgumentException if zkCurator is null
     */
    public OpStoreImpl(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    /**
     * Ensures that the required entity type paths exist in ZooKeeper.
     * Creates missing paths if necessary.
     *
     * @throws MetaStoreException if path creation fails
     */
    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.ofEntityType(SUB_OP));
        zkMetaStore.createZNode(ZNode.ofEntityType(SHARD_OP));
    }

    /**
     * Creates a new subscription operation in the store.
     *
     * @param operation The subscription operation to create
     * @throws IllegalArgumentException if operation is null
     */
    @Override
    public void createSubOp(SubscriptionOperation operation) {
        ZNode znode = ZNode.ofSubOperation(operation.getName());
        zkMetaStore.createZNodeWithData(znode, operation);
    }

    /**
     * Retrieves a subscription operation by its ID.
     *
     * @param operationId The ID of the subscription operation
     * @return The subscription operation
     * @throws IllegalArgumentException if operation ID is null
     */
    @Override
    public SubscriptionOperation getSubOp(String operationId) {
        ZNode znode = ZNode.ofSubOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, SubscriptionOperation.class);
    }

    /**
     * Updates an existing subscription operation in the store.
     *
     * @param operation The subscription operation to update
     * @throws IllegalArgumentException if operation is null
     */
    @Override
    public void updateSubOp(SubscriptionOperation operation) {
        ZNode znode = ZNode.ofSubOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }

    /**
     * Creates a new shard operation in the store.
     *
     * @param operation The shard operation to create
     * @throws IllegalArgumentException if operation is null
     */
    @Override
    public void createShardOp(ShardOperation operation) {
        ZNode znode = ZNode.ofShardOperation(operation.getName());
        zkMetaStore.createZNodeWithData(znode, operation);
    }

    /**
     * Retrieves a shard operation by its ID.
     *
     * @param operationId The ID of the shard operation
     * @return The shard operation
     * @throws IllegalArgumentException if operation ID is null
     */
    @Override
    public ShardOperation getShardOp(String operationId) {
        ZNode znode = ZNode.ofShardOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, ShardOperation.class);
    }

    /**
     * Retrieves all shard operations associated with a specific subscription operation.
     *
     * @param subOpId The subscription operation ID
     * @return List of shard operations associated with the subscription
     */
    @Override
    public List<ShardOperation> getShardOps(String subOpId) {
        // TODO: This needs to improvise i.e shouldn't need to deserialize all.
        return zkMetaStore.listChildren(ZNode.ofEntityType(SHARD_OP))
                          .stream()
                          .map(id -> zkMetaStore.getZNodeDataAsPojo(ZNode.ofShardOperation(id), ShardOperation.class))
                          .filter(shardOp -> subOpId.equals(shardOp.getOpData().getParentOpId()))
                          .toList();
    }

    /**
     * Retrieves all pending subscription operations.
     *
     * @return List of pending subscription operations
     */
    @Override
    public List<SubscriptionOperation> getPendingSubOps() {
        // TODO: This needs to improvise i.e shouldn't need to deserialize all.
        return zkMetaStore.listChildren(ZNode.ofEntityType(SUB_OP))
                          .stream()
                          .map(
                              id -> zkMetaStore.getZNodeDataAsPojo(
                                  ZNode.ofSubOperation(id),
                                  SubscriptionOperation.class
                              )
                          )
                          .filter(subOp -> !subOp.isDone())
                          .toList();
    }

    /**
     * Checks if a shard operation exists in the store.
     *
     * @param shardOpId The ID of the shard operation
     * @return true if the shard operation exists, false otherwise
     * @throws IllegalArgumentException if shard operation ID is null
     */
    @Override
    public boolean shardOpExists(String shardOpId) {
        ZNode znode = ZNode.ofShardOperation(shardOpId);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing shard operation in the store.
     *
     * @param operation The shard operation to update
     * @throws IllegalArgumentException if operation is null
     */
    @Override
    public void updateShardOp(ShardOperation operation) {
        ZNode znode = ZNode.ofShardOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }
}
