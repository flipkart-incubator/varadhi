package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.web.TopicResource;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.ToxiproxyContainer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.cluster.ClusterSmokeIT.assumeDocker;
import static com.flipkart.varadhi.cluster.ClusterSmokeIT.awaitHealth;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 2-pod produce through nginx LB with Toxiproxy in front (latency + connection-cut “error”).
 *
 * <p>Path: {@code client → Toxiproxy → nginx LB → varadhi-0|varadhi-1}. Toxiproxy is network
 * middleware, not an in-process Varadhi filter.
 *
 * <pre>
 * ./gradlew :server:testCluster -Pvaradhi.cluster.tests=true \\
 *   --tests com.flipkart.varadhi.cluster.MultiPodProduceFaultIT
 * </pre>
 */
@EnabledIfSystemProperty(named = "varadhi.cluster.tests", matches = "true")
class MultiPodProduceFaultIT {

    private static final String HDR_MESSAGE_ID = "X_MESSAGE_ID";

    @Test
    void produceViaLbProxy_latencyThenCutThenRestore() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(2)
                                                                        .withLoadBalancer(true)
                                                                        .withToxiproxy(true)
                                                                        .build()) {
            cluster.start();
            assertEquals(2, cluster.podAliases().size());

            ToxiproxyContainer.ContainerProxy lbProxy = cluster.lbHttpProxy();
            try (ClusterHttpClient http = new ClusterHttpClient(cluster.loadBalancerBaseUriViaProxy())) {
                awaitHealth(http);

                String suffix = String.valueOf(System.currentTimeMillis());
                String org = "fault_org_" + suffix;
                String team = "fault_team";
                String project = "fault_project_" + suffix;
                String topic = "fault_topic";
                http.ensureOrgTeamProject(org, team, project);

                TopicResource resource = TopicResource.unGrouped(
                    topic,
                    project,
                    null,
                    LifecycleStatus.ActionCode.SYSTEM_ACTION,
                    "cluster-fault-e2e"
                );
                try (Response created = http.postJson("/v1/projects/" + project + "/topics", resource)) {
                    assertEquals(200, created.getStatus(), () -> created.readEntity(String.class));
                }
                http.awaitProduceReady(project, topic);

                // Baseline: produce reaches both pods through LB (no toxic yet).
                Set<String> upstreams = new HashSet<>();
                for (int i = 0; i < 20; i++) {
                    produceOk(http, project, topic, "base-" + i, upstreams);
                }
                assertTrue(upstreams.size() >= 2, "expected >=2 pods before faults; upstreams=" + upstreams);

                // Latency: produce still succeeds but is slowed by Toxiproxy.
                lbProxy.toxics().latency("produce-latency", ToxicDirection.DOWNSTREAM, 1_500);
                long start = System.nanoTime();
                produceOk(http, project, topic, "slow-1", null);
                long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
                assertTrue(
                    elapsedMs >= 1_200,
                    "expected >=~1.2s with 1500ms latency toxic on produce; was " + elapsedMs + "ms"
                );
                lbProxy.toxics().get("produce-latency").remove();

                // Error: cut the client↔LB path — produce must fail at the transport layer.
                lbProxy.setConnectionCut(true);
                assertThrows(ProcessingException.class, () -> {
                    try (Response ignored = http.produce(
                        project,
                        topic,
                        "cut".getBytes(),
                        Map.of(HDR_MESSAGE_ID, "cut-1")
                    )) {
                        // unreachable when connection is cut
                    }
                });

                // Restore: produce works again across pods.
                lbProxy.setConnectionCut(false);
                awaitHealth(http);
                http.awaitProduceReady(project, topic);
                Set<String> after = new HashSet<>();
                for (int i = 0; i < 20; i++) {
                    produceOk(http, project, topic, "restore-" + i, after);
                }
                assertTrue(after.size() >= 2, "expected >=2 pods after restore; upstreams=" + after);
            }
        }
    }

    private static void produceOk(
        ClusterHttpClient http,
        String project,
        String topic,
        String msgId,
        Set<String> upstreams
    ) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HDR_MESSAGE_ID, msgId);
        try (Response response = http.produce(project, topic, ("body-" + msgId).getBytes(), headers)) {
            assertEquals(200, response.getStatus(), () -> "produce failed: " + response.readEntity(String.class));
            if (upstreams != null) {
                String upstream = response.getHeaderString("X-Upstream-Addr");
                if (upstream != null && !upstream.isBlank()) {
                    upstreams.add(upstream);
                }
            }
        }
    }
}
