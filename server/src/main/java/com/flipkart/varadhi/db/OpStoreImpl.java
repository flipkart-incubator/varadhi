package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import org.apache.curator.framework.CuratorFramework;

import static com.flipkart.varadhi.db.ZNode.*;

public class OpStoreImpl implements OpStore {

    private final ZKMetaStore zkMetaStore;

    public OpStoreImpl(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.OfEntityType(SUB_OP));
        zkMetaStore.createZNode(ZNode.OfEntityType(SHARD_OP));
    }

    @Override
    public void createSubOp(SubscriptionOperation operation) {
        ZNode znode = ZNode.OfSubOperation(operation.getName());
        zkMetaStore.createZNodeWithData(znode, operation);
    }

    @Override
    public void createShardOp(ShardOperation operation) {
        ZNode znode = ZNode.OfShardOperation(operation.getName());
        zkMetaStore.createZNodeWithData(znode, operation);
    }

    @Override
    public SubscriptionOperation getSubOp(String operationId) {
        ZNode znode = ZNode.OfSubOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, SubscriptionOperation.class);
    }

    @Override
    public ShardOperation getShardOp(String operationId) {
        ZNode znode = ZNode.OfShardOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, ShardOperation.class);
    }

    @Override
    public void updateSubOp(SubscriptionOperation operation) {
        ZNode znode = ZNode.OfSubOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }

    @Override
    public void updateShardOp(ShardOperation operation) {
        ZNode znode = ZNode.OfShardOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }
}
