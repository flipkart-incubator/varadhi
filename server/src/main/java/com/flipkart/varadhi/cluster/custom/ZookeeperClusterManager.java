package com.flipkart.varadhi.cluster.custom;

import com.flipkart.varadhi.cluster.NodeResources;
import com.flipkart.varadhi.config.ZookeeperConnectConfig;
import com.flipkart.varadhi.utils.CuratorFrameworkCreator;
import io.vertx.core.Promise;
import io.vertx.core.spi.cluster.NodeInfo;
import lombok.extern.slf4j.Slf4j;

/**
 * Customized zkClusterManager that only works with provided zk configuration. The curatorFramework, needs to be
 * configured with namespace for the purpose of cluster nodes.
 *
 * In the future, we will customize this so that curator is not required to be namespace aware, instead this class will
 * handle the namespacing all the paths. This should allow usage of curator client for other purposes other than just cluster
 * management. We also need to remove curator.close() from leave method, in case the curator instance is provided.
 *
 * This class also customizes 1 method:
 * 1. setNodeInfo() - to include {@link NodeResources} as part of the nodeInfo
 */
@Slf4j
public class ZookeeperClusterManager extends io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager {

    /**
     * Node specific resource information to be used for load-balancing. This will be persisted as part of nodeInfo
     */
    private final NodeResources nodeResources;

    public ZookeeperClusterManager(ZookeeperConnectConfig zkConnectConfig, String nodeId, NodeResources nodeResources) {
        super(CuratorFrameworkCreator.create(zkConnectConfig), nodeId);
        this.nodeResources = nodeResources;
    }

    @Override
    public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
        nodeInfo.metadata().put("cpuCount", nodeResources.cpuCount());
        nodeInfo.metadata().put("nicMBps", nodeResources.nicMBps());
        super.setNodeInfo(nodeInfo, promise);
    }

    public NodeResources toNodeResources(NodeInfo nodeInfo) {
        return new NodeResources(
                nodeInfo.metadata().getInteger("cpuCount"),
                nodeInfo.metadata().getInteger("nicMBps")
        );
    }
}
