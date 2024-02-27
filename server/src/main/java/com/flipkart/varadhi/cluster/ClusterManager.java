package com.flipkart.varadhi.cluster;

import java.util.List;

/**
 * This interface is responsible for managing the cluster membership. Its implementation is responsible for capturing the
 * node resources, local ip & hostname, so that they can be used while registering to the cluster and other nodes can
 * be notified of the same.
 */
public interface ClusterManager {
    void join();

    void leave();

    List<NodeInfo> getAllMembers();

    void addMembershipListener(MembershipListener listener);

    MessageChannel connect(String nodeId);

    // TODO: Any publish to all methods?

    // TODO: lock & leader election related methods to go in here.
}
