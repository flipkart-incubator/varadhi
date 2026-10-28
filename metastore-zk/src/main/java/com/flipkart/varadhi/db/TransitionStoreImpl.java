package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.cluster.failover.ActiveTransition;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.TransitionStore;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.db.ZNode.TRANSITION;

/**
 * ZooKeeper-backed {@link TransitionStore}. All writes are <b>untracked</b> (no L1 fan-out): the
 * {@link ActiveTransition} is controller-only master state and must never reach a pod cache.
 * Keyed by {@code topicFqn}, so {@link #create} enforces one active transition per topic.
 */
public class TransitionStoreImpl implements TransitionStore {

    private final ZKMetaStore zkMetaStore;

    /**
     * @throws MetaStoreException if unable to create the required ZooKeeper entity-type path
     */
    public TransitionStoreImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
        zkMetaStore.createZNode(ZNode.ofEntityType(TRANSITION));
    }

    @Override
    public void create(ActiveTransition transition) {
        ZNode znode = ZNode.ofTransition(transition.getName());
        zkMetaStore.createZNodeWithData(znode, transition);
    }

    @Override
    public ActiveTransition get(String topicFqn) {
        ZNode znode = ZNode.ofTransition(topicFqn);
        return zkMetaStore.getZNodeDataAsPojo(znode, ActiveTransition.class);
    }

    @Override
    public boolean exists(String topicFqn) {
        return zkMetaStore.zkPathExist(ZNode.ofTransition(topicFqn));
    }

    @Override
    public void update(ActiveTransition transition) {
        ZNode znode = ZNode.ofTransition(transition.getName());
        zkMetaStore.updateZNodeWithData(znode, transition);
    }

    @Override
    public void delete(String topicFqn) {
        zkMetaStore.deleteZNode(ZNode.ofTransition(topicFqn));
    }

    @Override
    public List<ActiveTransition> listActive() {
        List<ActiveTransition> active = new ArrayList<>();
        for (String fqn : zkMetaStore.listChildren(ZNode.ofEntityType(TRANSITION))) {
            try {
                active.add(zkMetaStore.getZNodeDataAsPojo(ZNode.ofTransition(fqn), ActiveTransition.class));
            } catch (ResourceNotFoundException e) {
                // Child disappeared between list and get; skip it.
            }
        }
        return active;
    }
}
