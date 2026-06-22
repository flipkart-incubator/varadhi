package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.RegionName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterMembershipViewTest {

    private final InMemoryVaradhiClusterManager clusterManager = new InMemoryVaradhiClusterManager();
    private final ClusterMembershipView membership = new ClusterMembershipView(clusterManager);
    private final AtomicInteger changeNotifications = new AtomicInteger();

    @Test
    void snapshot_SeedsFromClusterManager() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1"), "consumer-1", consumer("consumer-1")));
        startWithChangeListener();

        assertEquals(2, membership.snapshot().size());
        assertTrue(membership.snapshot().containsKey("server-1"));
        assertEquals(1, changeNotifications.get());
    }

    @Test
    void snapshot_UpdatesOnJoinAndLeave() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        startWithChangeListener();
        changeNotifications.set(0);

        clusterManager.simulateJoin("server-2", server("server-2"));
        assertEquals(2, membership.snapshot().size());
        assertEquals(1, changeNotifications.get());

        clusterManager.simulateLeave("server-2");
        assertEquals(1, membership.snapshot().size());
        assertFalse(membership.snapshot().containsKey("server-2"));
        assertEquals(2, changeNotifications.get());
    }

    @Test
    void snapshot_KeepsLastKnownMembersOnSeedFailure() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        startWithChangeListener();
        assertEquals(1, membership.snapshot().size());

        clusterManager.failNextSeed();
        membership.start();

        assertEquals(1, membership.snapshot().size());
    }

    private void startWithChangeListener() {
        membership.addMembershipChangeListener(changeNotifications::incrementAndGet);
        membership.start();
    }

    private static MemberInfo server(String hostname) {
        return member(hostname, ComponentKind.Server);
    }

    private static MemberInfo consumer(String hostname) {
        return member(hostname, ComponentKind.Consumer);
    }

    private static MemberInfo member(String hostname, ComponentKind role) {
        return new MemberInfo(
            hostname,
            "127.0.0.1",
            8080,
            new ComponentKind[] {role},
            new NodeCapacity(1000, 1000),
            RegionName.BOOTSTRAP_REGION
        );
    }
}
