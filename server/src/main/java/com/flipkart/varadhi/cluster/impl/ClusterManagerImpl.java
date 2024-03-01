package com.flipkart.varadhi.cluster.impl;

import com.flipkart.varadhi.cluster.*;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class ClusterManagerImpl implements ClusterManager {

    // TODO: add instance of zkClusterManager & clusteredVertx and use it to implement the methods of this class

    @Override
    public List<NodeInfo> getAllMembers() {
        return null;
    }

    @Override
    public void addMembershipListener(MembershipListener listener) {

    }

    @Override
    public MessageChannel connect(String nodeId) {
        return null;
    }
}
