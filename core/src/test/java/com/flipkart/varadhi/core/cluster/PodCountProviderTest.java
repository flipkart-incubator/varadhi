package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.entities.RegionName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PodCountProviderTest {

    private final InMemoryVaradhiClusterManager clusterManager = new InMemoryVaradhiClusterManager();
    private final ClusterMembershipView membership = new ClusterMembershipView(clusterManager);
    private final AtomicInteger countChangeNotifications = new AtomicInteger();

    @Test
    void withRole_CountsOnlyMatchingMembers() {
        clusterManager.replaceMembers(
            Map.of("server-1", server("server-1"), "server-2", server("server-2"), "consumer-1", consumer("consumer-1"))
        );
        PodCountProvider podCount = createPodCount(
            () -> PodCountProvider.withRole(membership, ComponentKind.Server, 1)
        );

        assertEquals(2, podCount.getAsInt());
        assertEquals(0, countChangeNotifications.get());
    }

    @Test
    void withRole_FloorsAtMinCount() {
        clusterManager.replaceMembers(Map.of("consumer-1", consumer("consumer-1")));
        PodCountProvider podCount = createPodCount(
            () -> PodCountProvider.withRole(membership, ComponentKind.Server, 1)
        );

        assertEquals(1, podCount.getAsInt());
    }

    @Test
    void withRole_UpdatesOnMatchingJoinAndLeave() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        PodCountProvider podCount = createPodCount(
            () -> PodCountProvider.withRole(membership, ComponentKind.Server, 1)
        );
        countChangeNotifications.set(0);

        clusterManager.simulateJoin("server-2", server("server-2"));
        assertEquals(2, podCount.getAsInt());
        assertEquals(1, countChangeNotifications.get());

        clusterManager.simulateLeave("server-2");
        assertEquals(1, podCount.getAsInt());
        assertEquals(2, countChangeNotifications.get());
    }

    @Test
    void withRole_IgnoresNonMatchingJoinAndLeave() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        PodCountProvider podCount = createPodCount(
            () -> PodCountProvider.withRole(membership, ComponentKind.Server, 1)
        );
        countChangeNotifications.set(0);

        clusterManager.simulateJoin("consumer-2", consumer("consumer-2"));
        clusterManager.simulateLeave("consumer-2");

        assertEquals(1, podCount.getAsInt());
        assertEquals(0, countChangeNotifications.get());
    }

    @Test
    void all_CountsEveryMember() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1"), "consumer-1", consumer("consumer-1")));
        PodCountProvider podCount = createPodCount(() -> PodCountProvider.all(membership));

        assertEquals(2, podCount.getAsInt());
    }

    @Test
    void all_UpdatesWhenAnyMemberJoinsOrLeaves() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1")));
        PodCountProvider podCount = createPodCount(() -> PodCountProvider.all(membership));
        countChangeNotifications.set(0);

        clusterManager.simulateJoin("consumer-1", consumer("consumer-1"));
        assertEquals(2, podCount.getAsInt());
        assertEquals(1, countChangeNotifications.get());
    }

    @Test
    void keepsLastKnownCountOnSeedFailure() {
        clusterManager.replaceMembers(Map.of("server-1", server("server-1"), "server-2", server("server-2")));
        PodCountProvider podCount = createPodCount(
            () -> PodCountProvider.withRole(membership, ComponentKind.Server, 1)
        );
        assertEquals(2, podCount.getAsInt());

        clusterManager.failNextSeed();
        membership.start();

        assertEquals(2, podCount.getAsInt());
    }

    private PodCountProvider createPodCount(Supplier<PodCountProvider> factory) {
        membership.start();
        PodCountProvider podCount = factory.get();
        podCount.addCountChangeListener(countChangeNotifications::incrementAndGet);
        return podCount;
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
