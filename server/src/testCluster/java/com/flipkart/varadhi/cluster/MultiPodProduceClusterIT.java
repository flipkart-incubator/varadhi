package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.web.TopicResource;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.cluster.ClusterSmokeIT.assumeDocker;
import static com.flipkart.varadhi.cluster.ClusterSmokeIT.awaitHealth;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-pod produce through an explicit nginx LB (not an automatic cloud ELB).
 *
 * <p>Nginx adds {@code X-Upstream-Addr} so we can prove round-robin reached more than one pod.
 *
 * <pre>
 * ./gradlew :server:testCluster -Pvaradhi.cluster.tests=true \\
 *   -Pvaradhi.cluster.image=varadhi.docker.registry/varadhi:latest
 * </pre>
 */
@EnabledIfSystemProperty(named = "varadhi.cluster.tests", matches = "true")
class MultiPodProduceClusterIT {

    private static final String HDR_MESSAGE_ID = "X_MESSAGE_ID";

    @Test
    void produceViaLb_hitsMultiplePods() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(2)
                                                                        .withLoadBalancer(true)
                                                                        .build()) {
            cluster.start();
            assertEquals(2, cluster.podAliases().size());

            try (ClusterHttpClient http = new ClusterHttpClient(cluster.loadBalancerBaseUri())) {
                awaitHealth(http);

                String suffix = String.valueOf(System.currentTimeMillis());
                String org = "cluster_org_" + suffix;
                String team = "cluster_team";
                String project = "cluster_project_" + suffix;
                String topic = "cluster_topic";
                http.ensureOrgTeamProject(org, team, project);

                TopicResource resource = TopicResource.unGrouped(
                    topic,
                    project,
                    null,
                    LifecycleStatus.ActionCode.SYSTEM_ACTION,
                    "cluster-e2e"
                );
                try (Response created = http.postJson("/v1/projects/" + project + "/topics", resource)) {
                    assertEquals(200, created.getStatus(), () -> created.readEntity(String.class));
                }

                http.awaitProduceReady(project, topic);

                Set<String> upstreams = new HashSet<>();
                for (int i = 0; i < 40; i++) {
                    Map<String, String> headers = new HashMap<>();
                    headers.put(HDR_MESSAGE_ID, "cluster-msg-" + i);
                    try (Response response = http.produce(project, topic, ("body-" + i).getBytes(), headers)) {
                        assertEquals(200, response.getStatus(), () -> "produce failed: " + response.readEntity(String.class));
                        String upstream = response.getHeaderString("X-Upstream-Addr");
                        if (upstream != null && !upstream.isBlank()) {
                            upstreams.add(upstream);
                        }
                    }
                }

                assertTrue(
                    upstreams.size() >= 2,
                    "expected LB to route to >=2 pods; upstreams seen=" + upstreams
                        + " (pods are not auto-ELB'd — nginx was started explicitly for this)"
                );
            }
        }
    }

    @Test
    void produceDirectlyToEachPod() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(2)
                                                                        .withLoadBalancer(false)
                                                                        .build()) {
            cluster.start();

            String suffix = String.valueOf(System.currentTimeMillis());
            String org = "direct_org_" + suffix;
            String team = "direct_team";
            String project = "direct_project_" + suffix;
            String topic = "direct_topic";

            // Admin APIs via first pod; produce to every pod.
            String first = cluster.podAliases().getFirst();
            try (ClusterHttpClient admin = new ClusterHttpClient(cluster.podBaseUri(first))) {
                awaitHealth(admin);
                admin.ensureOrgTeamProject(org, team, project);
                TopicResource resource = TopicResource.unGrouped(
                    topic,
                    project,
                    null,
                    LifecycleStatus.ActionCode.SYSTEM_ACTION,
                    "cluster-e2e"
                );
                try (Response created = admin.postJson("/v1/projects/" + project + "/topics", resource)) {
                    assertEquals(200, created.getStatus(), () -> created.readEntity(String.class));
                }
                admin.awaitProduceReady(project, topic);
            }

            for (String alias : cluster.podAliases()) {
                try (ClusterHttpClient http = new ClusterHttpClient(cluster.podBaseUri(alias))) {
                    // Each pod has its own cache; wait until ZK-backed entities are readable here.
                    http.awaitProduceReady(project, topic);
                    Map<String, String> headers = Map.of(HDR_MESSAGE_ID, "direct-" + alias);
                    try (Response response = http.produce(project, topic, "ping".getBytes(), headers)) {
                        assertEquals(
                            200,
                            response.getStatus(),
                            () -> "produce to " + alias + " failed: " + response.readEntity(String.class)
                        );
                    }
                }
            }
        }
    }
}
