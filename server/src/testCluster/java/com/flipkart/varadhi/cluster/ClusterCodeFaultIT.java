package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.web.TopicResource;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Map;

import static com.flipkart.varadhi.cluster.ClusterSmokeIT.assumeDocker;
import static com.flipkart.varadhi.cluster.ClusterSmokeIT.awaitHealth;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * In-JVM code faults via Byteman agent inside the Varadhi container.
 *
 * <p>Nginx LB is <strong>not</strong> required — hit the pod directly via {@code podBaseUri}.
 *
 * <pre>
 * ./gradlew :server:testCluster -Pvaradhi.cluster.tests=true
 * </pre>
 */
@EnabledIfSystemProperty(named = "varadhi.cluster.tests", matches = "true")
class ClusterCodeFaultIT {

    private static final String HDR_MESSAGE_ID = "X_MESSAGE_ID";

    /**
     * Boot-time Byteman rule flips health via {@code org.jboss.byteman.failHealth} (toggled through the
     * Byteman listener). Proves in-container code fault injection for e2e.
     */
    @Test
    void byteman_healthCheckThrowsThenClears() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(1)
                                                                        .withLoadBalancer(false)
                                                                        .withByteman(true)
                                                                        .build()) {
            cluster.start();
            String alias = cluster.podAliases().getFirst();
            try (ClusterHttpClient http = new ClusterHttpClient(cluster.podBaseUri(alias))) {
                awaitHealth(http);
                try (var ok = http.get("/v1/health-check")) {
                    assertEquals(200, ok.getStatus());
                    assertTrue(ok.readEntity(String.class).contains("iam_ok"));
                }

                BytemanControl byteman = cluster.byteman(alias);
                assertTrue(
                    byteman.listRules().contains("varadhi fail health check"),
                    "boot-time Byteman script should be loaded; agent=" + byteman.agentVersion()
                );

                byteman.setFailHealth(true);
                try (var failed = http.get("/v1/health-check")) {
                    int status = failed.getStatus();
                    String body = failed.readEntity(String.class);
                    assertFalse(
                        status == 200 && body.contains("iam_ok"),
                        "health should fail while Byteman fault is armed; status=" + status + " body=" + body
                    );
                }

                byteman.setFailHealth(false);
                awaitHealth(http);
                try (var ok = http.get("/v1/health-check")) {
                    assertEquals(200, ok.getStatus());
                    assertTrue(ok.readEntity(String.class).contains("iam_ok"));
                }
            }
        }
    }

    /**
     * Forces {@code ResourceNotFoundException} on produce (HTTP 404) even though the topic exists.
     * No nginx — single pod via {@code podBaseUri}.
     */
    @Test
    void byteman_produceNotFoundThenClears() throws Exception {
        assumeDocker();
        try (VaradhiClusterDeployment cluster = VaradhiClusterDeployment.builder()
                                                                        .initialPods(1)
                                                                        .withLoadBalancer(false)
                                                                        .withByteman(true)
                                                                        .build()) {
            cluster.start();
            String alias = cluster.podAliases().getFirst();
            try (ClusterHttpClient http = new ClusterHttpClient(cluster.podBaseUri(alias))) {
                awaitHealth(http);

                String suffix = String.valueOf(System.currentTimeMillis());
                String org = "bnf_org_" + suffix;
                String team = "bnf_team";
                String project = "bnf_project_" + suffix;
                String topic = "bnf_topic";
                http.ensureOrgTeamProject(org, team, project);
                TopicResource resource = TopicResource.unGrouped(
                    topic,
                    project,
                    null,
                    LifecycleStatus.ActionCode.SYSTEM_ACTION,
                    "byteman-not-found-e2e"
                );
                try (Response created = http.postJson("/v1/projects/" + project + "/topics", resource)) {
                    assertEquals(200, created.getStatus(), () -> created.readEntity(String.class));
                }
                http.awaitProduceReady(project, topic);

                BytemanControl byteman = cluster.byteman(alias);
                assertTrue(
                    byteman.listRules().contains("varadhi fail produce not found"),
                    "produce-not-found rule missing; rules=" + byteman.listRules()
                );

                try (Response ok = http.produce(
                    project,
                    topic,
                    "before".getBytes(),
                    Map.of(HDR_MESSAGE_ID, "before-fault")
                )) {
                    assertEquals(200, ok.getStatus(), () -> ok.readEntity(String.class));
                }

                byteman.setFailProduceNotFound(true);
                try (Response notFound = http.produce(
                    project,
                    topic,
                    "during".getBytes(),
                    Map.of(HDR_MESSAGE_ID, "during-fault")
                )) {
                    String body = notFound.readEntity(String.class);
                    assertEquals(404, notFound.getStatus(), () -> body);
                    assertTrue(body.toLowerCase().contains("not found"), "expected not-found body: " + body);
                }

                byteman.setFailProduceNotFound(false);
                http.awaitProduceReady(project, topic);
                try (Response ok = http.produce(
                    project,
                    topic,
                    "after".getBytes(),
                    Map.of(HDR_MESSAGE_ID, "after-fault")
                )) {
                    assertEquals(200, ok.getStatus(), () -> ok.readEntity(String.class));
                }
            }
        }
    }
}
