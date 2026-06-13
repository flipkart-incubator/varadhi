package com.flipkart.varadhi.produce.ratelimit;

import com.flipkart.varadhi.core.cluster.ClusterMembershipView;
import com.flipkart.varadhi.core.cluster.ComponentKind;
import com.flipkart.varadhi.core.cluster.FakeVaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.MemberInfo;
import com.flipkart.varadhi.core.cluster.NodeCapacity;
import com.flipkart.varadhi.core.cluster.PodCountProvider;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ProduceRateLimiterTest {

    private static final String REGION = "local";

    @Test
    void resolveLimiter_ReusesRegistryEntryPerTopic() {
        ProduceRateLimiter facade = facadeWithPodCount(2);
        VaradhiTopic topic = topic();

        TopicRateLimiter first = facade.resolveLimiter(topic);
        TopicRateLimiter second = facade.resolveLimiter(topic);

        assertSame(first, second);
    }

    @Test
    void resolveLimiter_RefreshesQuotaLazilyOnMembershipChange() {
        FakeVaradhiClusterManager clusterManager = new FakeVaradhiClusterManager();
        clusterManager.replaceMembers(Map.of("server-1", server("server-1"), "server-2", server("server-2")));
        PodCountProvider podCount = startServerPodCount(clusterManager);

        EvenSplitPerPodTopicQuotaProvider quotaProvider = new EvenSplitPerPodTopicQuotaProvider(
            REGION,
            0.25,
            1,
            podCount
        );
        ProduceRateLimiter facade = new ProduceRateLimiter(quotaProvider, 1, new AtomicLong(0L)::get, podCount);
        VaradhiTopic topic = topic();

        PerPodTopicQuota before = facade.resolveLimiter(topic).lastQuota();

        clusterManager.simulateJoin("server-3", server("server-3"));
        PerPodTopicQuota after = facade.resolveLimiter(topic).lastQuota();

        assertNotEquals(before, after);
    }

    private static ProduceRateLimiter facadeWithPodCount(int servers) {
        FakeVaradhiClusterManager clusterManager = new FakeVaradhiClusterManager();
        Map<String, MemberInfo> members = new java.util.HashMap<>();
        for (int i = 0; i < servers; i++) {
            members.put("server-" + i, server("server-" + i));
        }
        clusterManager.replaceMembers(members);
        PodCountProvider podCount = startServerPodCount(clusterManager);
        EvenSplitPerPodTopicQuotaProvider quotaProvider = new EvenSplitPerPodTopicQuotaProvider(
            REGION,
            0.25,
            1,
            podCount
        );
        return new ProduceRateLimiter(quotaProvider, 1, new AtomicLong(0L)::get, podCount);
    }

    private static PodCountProvider startServerPodCount(FakeVaradhiClusterManager clusterManager) {
        ClusterMembershipView membership = new ClusterMembershipView(clusterManager);
        PodCountProvider podCount = PodCountProvider.withRole(membership, ComponentKind.Server, 1);
        podCount.start();
        return podCount;
    }

    private static VaradhiTopic topic() {
        return VaradhiTopic.of(
            "project",
            "topic",
            false,
            new TopicCapacityPolicy(100, 1000, 1, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC,
            Map.of(REGION, 1.0),
            null,
            null
        );
    }

    private static MemberInfo server(String hostname) {
        return new MemberInfo(
            hostname,
            "127.0.0.1",
            8080,
            new ComponentKind[] {ComponentKind.Server},
            new NodeCapacity(1000, 1000),
            RegionName.BOOTSTRAP_REGION
        );
    }
}
