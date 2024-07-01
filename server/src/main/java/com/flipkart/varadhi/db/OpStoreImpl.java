package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;

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
    public SubscriptionOperation getSubOp(String operationId) {
        ZNode znode = ZNode.OfSubOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, SubscriptionOperation.class);
    }

    @Override
    public void updateSubOp(SubscriptionOperation operation) {
        ZNode znode = ZNode.OfSubOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }

    @Override
    public void createShardOp(ShardOperation operation) {
        ZNode znode = ZNode.OfShardOperation(operation.getName());
        zkMetaStore.createZNodeWithData(znode, operation);
    }

    @Override
    public List<ShardOperation> getShardOps(String operationId) {
        //TODO::implement this.
        //TODO::This needs to improvise i.e shouldn't need to deserialize all.
        List<String> shardOpIds = zkMetaStore.listChildren(ZNode.OfEntityType(SHARD_OP));
        List<ShardOperation> shardOps = new ArrayList<>();
        shardOpIds.forEach(id -> {
            ShardOperation shardOp = zkMetaStore.getZNodeDataAsPojo(ZNode.OfShardOperation(id), ShardOperation.class);
            if (operationId.equals(shardOp.getOpData().getParentOpId())) {
                shardOps.add(shardOp);
            }
        });
        return shardOps;
    }
    @Override
    public ShardOperation getShardOp(String operationId) {
        ZNode znode = ZNode.OfShardOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, ShardOperation.class);
    }

    @Override
    public void updateShardOp(ShardOperation operation) {
        ZNode znode = ZNode.OfShardOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }
}
