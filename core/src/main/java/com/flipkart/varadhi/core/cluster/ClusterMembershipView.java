package com.flipkart.varadhi.core.cluster;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Live, in-memory index of cluster members keyed by node id.
 * <p>
 * Seeds from {@link VaradhiClusterManager#getMembersByNodeId()} and stays current via
 * {@link MembershipListener}. Intended as a fast local view until membership caching moves into
 * {@link VaradhiZkClusterManager}.
 */
@Slf4j
public final class ClusterMembershipView {

    private final VaradhiClusterManager clusterManager;
    private final ConcurrentHashMap<String, MemberInfo> membersByNodeId = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Runnable> changeListeners = new CopyOnWriteArrayList<>();

    public ClusterMembershipView(VaradhiClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Registers the membership listener and seeds from the cluster snapshot. Idempotent if called
     * more than once — each call re-registers the listener and re-seeds.
     */
    public void start() {
        clusterManager.addMembershipListener(membershipListener);
        clusterManager.getMembersByNodeId()
                      .onSuccess(this::seedMembers)
                      .onFailure(
                          e -> log.warn(
                              "Failed to seed cluster membership view; keeping last-known snapshot of {} members",
                              membersByNodeId.size(),
                              e
                          )
                      );
    }

    /** Unmodifiable snapshot of the current membership index. */
    public Map<String, MemberInfo> snapshot() {
        return Map.copyOf(membersByNodeId);
    }

    public void addMembershipChangeListener(Runnable listener) {
        changeListeners.add(listener);
    }

    private final MembershipListener membershipListener = new MembershipListener() {
        @Override
        public CompletableFuture<Void> joined(String nodeId, MemberInfo memberInfo) {
            membersByNodeId.put(nodeId, memberInfo);
            notifyChangeListeners();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> left(String nodeId) {
            membersByNodeId.remove(nodeId);
            notifyChangeListeners();
            return CompletableFuture.completedFuture(null);
        }
    };

    private void seedMembers(Map<String, MemberInfo> members) {
        membersByNodeId.clear();
        membersByNodeId.putAll(members);
        notifyChangeListeners();
    }

    private void notifyChangeListeners() {
        changeListeners.forEach(Runnable::run);
    }
}
