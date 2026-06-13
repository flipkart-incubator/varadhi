package com.flipkart.varadhi.core.cluster;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;
import java.util.Map;

/**
 * This interface is responsible for managing the cluster membership. Its implementation is responsible for capturing
 * the
 * node resources, local ip & hostname, so that they can be used while registering to the cluster and other nodes can
 * be notified of the same.
 */
public interface VaradhiClusterManager {

    Future<List<MemberInfo>> getAllMembers();

    /** Cluster node id → member snapshot (same source as {@link #getAllMembers()}). */
    Future<Map<String, MemberInfo>> getMembersByNodeId();

    void addMembershipListener(MembershipListener listener);

    MessageRouter getRouter(Vertx vertx);

    MessageExchange getExchange(Vertx vertx);

    // TODO: lock & leader election related methods to go in here.
}
