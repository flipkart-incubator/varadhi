package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.core.cluster.entities.MemberInfo;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;

/**
 * This interface is responsible for managing the cluster membership. Its implementation is responsible for capturing
 * the
 * node resources, local ip & hostname, so that they can be used while registering to the cluster and other nodes can
 * be notified of the same.
 */
public interface VaradhiClusterManager {

    Future<List<MemberInfo>> getAllMembers();

    void addMembershipListener(MembershipListener listener);

    MessageRouter getRouter(Vertx vertx);

    MessageExchange getExchange(Vertx vertx);

    // TODO: lock & leader election related methods to go in here.
}
