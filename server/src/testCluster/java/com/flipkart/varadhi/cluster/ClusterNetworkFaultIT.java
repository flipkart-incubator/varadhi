package com.flipkart.varadhi.cluster;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import jakarta.ws.rs.ProcessingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.ToxiproxyContainer;

import static com.flipkart.varadhi.cluster.ClusterSmokeIT.assumeDocker;
import static com.flipkart.varadhi.cluster.ClusterSmokeIT.awaitHealth;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Network faults via Toxiproxy on the client → Varadhi HTTP path.
 *
 * <pre>
 * ./gradlew :server:testCluster -Pvaradhi.cluster.tests=true
 * </pre>
 */
@EnabledIfSystemProperty(named = "varadhi.cluster.tests", matches = "true")
class ClusterNetworkFaultIT {

    @Test
    void cutConnection_blocksHealthThenRestores() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(1)
                                                                        .withLoadBalancer(false)
                                                                        .withToxiproxy(true)
                                                                        .build()) {
            cluster.start();
            String alias = cluster.podAliases().getFirst();
            ToxiproxyContainer.ContainerProxy proxy = cluster.httpProxy(alias);

            try (ClusterHttpClient viaProxy = new ClusterHttpClient(cluster.podBaseUriViaProxy(alias))) {
                awaitHealth(viaProxy);
                try (var ok = viaProxy.get("/v1/health-check")) {
                    assertEquals(200, ok.getStatus());
                }

                proxy.setConnectionCut(true);
                assertThrows(ProcessingException.class, () -> {
                    try (var ignored = viaProxy.get("/v1/health-check")) {
                        // connection cut — client should fail to talk to the pod
                    }
                });

                proxy.setConnectionCut(false);
                awaitHealth(viaProxy);
                try (var ok = viaProxy.get("/v1/health-check")) {
                    assertEquals(200, ok.getStatus());
                    assertTrue(ok.readEntity(String.class).contains("iam_ok"));
                }
            }
        }
    }

    @Test
    void latencyToxic_slowsHealthCheck() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(1)
                                                                        .withLoadBalancer(false)
                                                                        .withToxiproxy(true)
                                                                        .build()) {
            cluster.start();
            String alias = cluster.podAliases().getFirst();
            ToxiproxyContainer.ContainerProxy proxy = cluster.httpProxy(alias);

            try (ClusterHttpClient viaProxy = new ClusterHttpClient(cluster.podBaseUriViaProxy(alias))) {
                awaitHealth(viaProxy);

                proxy.toxics().latency("health-latency", ToxicDirection.DOWNSTREAM, 1_500);

                long start = System.nanoTime();
                try (var ok = viaProxy.get("/v1/health-check")) {
                    assertEquals(200, ok.getStatus());
                }
                long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
                assertTrue(elapsedMs >= 1_200, "expected >=~1.2s with 1500ms latency toxic; was " + elapsedMs + "ms");

                proxy.toxics().get("health-latency").remove();
            }
        }
    }
}
