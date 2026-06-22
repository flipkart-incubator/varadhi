package com.flipkart.varadhi.core.cluster;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** In-memory {@link VaradhiClusterManager} for unit tests. */
public final class InMemoryVaradhiClusterManager implements VaradhiClusterManager {

    private final Map<String, MemberInfo> members = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<MembershipListener> listeners = new CopyOnWriteArrayList<>();
    private volatile boolean failNextSeed;

    public void replaceMembers(Map<String, MemberInfo> snapshot) {
        members.clear();
        members.putAll(snapshot);
    }

    public void simulateJoin(String nodeId, MemberInfo memberInfo) {
        members.put(nodeId, memberInfo);
        listeners.forEach(listener -> listener.joined(nodeId, memberInfo));
    }

    public void simulateLeave(String nodeId) {
        members.remove(nodeId);
        listeners.forEach(listener -> listener.left(nodeId));
    }

    public void failNextSeed() {
        failNextSeed = true;
    }

    @Override
    public Future<List<MemberInfo>> getAllMembers() {
        return getMembersByNodeId().map(snapshot -> List.copyOf(snapshot.values()));
    }

    @Override
    public Future<Map<String, MemberInfo>> getMembersByNodeId() {
        if (failNextSeed) {
            failNextSeed = false;
            return Future.failedFuture("zk hiccup");
        }
        return Future.succeededFuture(Map.copyOf(members));
    }

    @Override
    public void addMembershipListener(MembershipListener listener) {
        listeners.add(listener);
    }

    @Override
    public MessageRouter getRouter(Vertx vertx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageExchange getExchange(Vertx vertx) {
        throw new UnsupportedOperationException();
    }
}
