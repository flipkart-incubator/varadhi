package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.TransitionStore;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.TRANSITION;

/**
 * ZooKeeper-backed {@link TransitionStore}. All writes are <b>untracked</b> (no L1 fan-out): the
 * {@link TransitionObject} is controller-only master state and must never reach a pod cache.
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
    public void create(TransitionObject transition) {
        ZNode znode = ZNode.ofTransition(transition.getName());
        zkMetaStore.createZNodeWithData(znode, transition);
    }

    @Override
    public TransitionObject get(String topicFqn) {
        ZNode znode = ZNode.ofTransition(topicFqn);
        return zkMetaStore.getZNodeDataAsPojo(znode, TransitionObject.class);
    }

    @Override
    public boolean exists(String topicFqn) {
        return zkMetaStore.zkPathExist(ZNode.ofTransition(topicFqn));
    }

    @Override
    public void update(TransitionObject transition) {
        ZNode znode = ZNode.ofTransition(transition.getName());
        zkMetaStore.updateZNodeWithData(znode, transition);
    }

    @Override
    public void delete(String topicFqn) {
        zkMetaStore.deleteZNode(ZNode.ofTransition(topicFqn));
    }

    @Override
    public List<TransitionObject> listActive() {
        return zkMetaStore.listChildren(ZNode.ofEntityType(TRANSITION))
                          .stream()
                          .map(fqn -> zkMetaStore.getZNodeDataAsPojo(ZNode.ofTransition(fqn), TransitionObject.class))
                          .toList();
    }
}
