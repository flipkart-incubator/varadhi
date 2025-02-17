package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.OpStore;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.db.ZNode.SHARD_OP;
import static com.flipkart.varadhi.db.ZNode.SUB_OP;

public class OpStoreImpl implements OpStore {

    private final ZKMetaStore zkMetaStore;

    public OpStoreImpl(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.ofEntityType(SUB_OP));
        zkMetaStore.createZNode(ZNode.ofEntityType(SHARD_OP));
    }

    @Override
    public void createSubOp(SubscriptionOperation operation) {
        ZNode znode = ZNode.ofSubOperation(operation.getName());
        zkMetaStore.createZNodeWithData(znode, operation);
    }

    @Override
    public SubscriptionOperation getSubOp(String operationId) {
        ZNode znode = ZNode.ofSubOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, SubscriptionOperation.class);
    }

    @Override
    public void updateSubOp(SubscriptionOperation operation) {
        ZNode znode = ZNode.ofSubOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }

    @Override
    public void createShardOp(ShardOperation operation) {
        ZNode znode = ZNode.ofShardOperation(operation.getName());
        zkMetaStore.createZNodeWithData(znode, operation);
    }

    @Override
    public boolean shardOpExists(String shardOpId) {
        ZNode znode = ZNode.ofShardOperation(shardOpId);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public List<ShardOperation> getShardOps(String subOpId) {
        //TODO::implement this.
        //TODO::This needs to improvise i.e shouldn't need to deserialize all.
        List<String> shardOpIds = zkMetaStore.listChildren(ZNode.ofEntityType(SHARD_OP));
        List<ShardOperation> shardOps = new ArrayList<>();
        shardOpIds.forEach(id -> {
            ShardOperation shardOp = zkMetaStore.getZNodeDataAsPojo(ZNode.ofShardOperation(id), ShardOperation.class);
            if (subOpId.equals(shardOp.getOpData().getParentOpId())) {
                shardOps.add(shardOp);
            }
        });
        return shardOps;
    }

    @Override
    public List<SubscriptionOperation> getPendingSubOps() {
        //TODO::implement this.
        //TODO::This needs to improvise i.e shouldn't need to deserialize all.
        List<String> subOpIds = zkMetaStore.listChildren(ZNode.ofEntityType(SUB_OP));
        List<SubscriptionOperation> subOps = new ArrayList<>();
        subOpIds.forEach(id -> {
            SubscriptionOperation subOp = zkMetaStore.getZNodeDataAsPojo(
                ZNode.ofSubOperation(id),
                SubscriptionOperation.class
            );
            if (!subOp.isDone()) {
                subOps.add(subOp);
            }
        });
        return subOps;
    }


    @Override
    public ShardOperation getShardOp(String operationId) {
        ZNode znode = ZNode.ofShardOperation(operationId);
        return zkMetaStore.getZNodeDataAsPojo(znode, ShardOperation.class);
    }

    @Override
    public void updateShardOp(ShardOperation operation) {
        ZNode znode = ZNode.ofShardOperation(operation.getName());
        zkMetaStore.updateZNodeWithData(znode, operation);
    }
}
