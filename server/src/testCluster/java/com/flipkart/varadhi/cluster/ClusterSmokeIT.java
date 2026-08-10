package com.flipkart.varadhi.cluster;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boots ZK + Pulsar + one Varadhi pod and asserts health.
 *
 * <pre>
 * ./gradlew :server:testCluster -Pvaradhi.cluster.tests=true
 * </pre>
 */
@EnabledIfSystemProperty(named = "varadhi.cluster.tests", matches = "true")
class ClusterSmokeIT {

    @Test
    void singlePod_healthCheckOk() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(1)
                                                                        .withLoadBalancer(false)
                                                                        .build()) {
            cluster.start();
            String alias = cluster.podAliases().getFirst();
            try (ClusterHttpClient http = new ClusterHttpClient(cluster.podBaseUri(alias))) {
                awaitHealth(http);
                try (var response = http.get("/v1/health-check")) {
                    assertEquals(200, response.getStatus());
                    assertTrue(response.readEntity(String.class).contains("iam_ok"));
                }
            }
        }
    }

    static void assumeDocker() {
        Assumptions.assumeTrue(
            DockerClientFactory.instance().isDockerAvailable(),
            "Docker required for cluster tests"
        );
    }

    static void awaitHealth(ClusterHttpClient http) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (System.nanoTime() < deadline) {
            try (var response = http.get("/v1/health-check")) {
                if (response.getStatus() == 200) {
                    return;
                }
            } catch (Exception ignored) {
                // still starting
            }
            Thread.sleep(500);
        }
        throw new AssertionError("health-check not ready for " + http.baseUri());
    }
}
