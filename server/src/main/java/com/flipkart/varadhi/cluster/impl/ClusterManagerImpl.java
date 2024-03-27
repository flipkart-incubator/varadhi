package com.flipkart.varadhi.cluster.impl;


import com.flipkart.varadhi.cluster.ClusterManager;
import com.flipkart.varadhi.cluster.MembershipListener;
import com.flipkart.varadhi.cluster.MessageChannelImpl;
import com.flipkart.varadhi.cluster.NodeInfo;
import com.flipkart.varadhi.core.cluster.MessageChannel;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;

import java.util.List;


@RequiredArgsConstructor
public class ClusterManagerImpl implements ClusterManager {
    // TODO: add instance of zkClusterManager & clusteredVertx and use it to implement the methods of this class
    private final Vertx vertx;
    @Override
    public List<NodeInfo> getAllMembers() {
        return null;
    }

    @Override
    public void addMembershipListener(MembershipListener listener) {

    }

    @Override
    public MessageChannel connect(String nodeId) {
        return new MessageChannelImpl(vertx.eventBus());
    }
}
