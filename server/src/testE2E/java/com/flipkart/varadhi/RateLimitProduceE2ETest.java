package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import com.flipkart.varadhi.entities.web.TopicResource;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * VIP-0001 §16 produce-path E2E against a running server ({@code rateLimiterOptions.enabled=true} in
 * docker config). HTTP/2 streaming and multi-pod even-split scenarios are covered at integration level.
 */
public class RateLimitProduceE2ETest extends E2EBase {

    private static final String HDR_MESSAGE_ID = "X_MESSAGE_ID";
    private static final String RATE_LIMIT_MSG =
        "Produce to Topic/Queue is currently rate limited, try again after sometime.";

    private static Org org;
    private static Team team;
    private static Project project;
    private static final List<String> createdTopics = new ArrayList<>();

    @BeforeAll
    public static void setup() throws InterruptedException {
        long suffix = System.currentTimeMillis();
        org = Org.of("rate_limit_org_" + suffix);
        team = Team.of("rate_limit_team_" + suffix, org.getName());
        project = Project.of("rate_limit_project_" + suffix, "", team.getName(), team.getOrg());
        makeCreateRequest(getOrgsUri(), org, EXPECTED_STATUS_OK);
        makeCreateRequest(getTeamsUri(team.getOrg()), team, EXPECTED_STATUS_OK);
        makeCreateRequest(getProjectCreateUri(), project, EXPECTED_STATUS_OK);
        Thread.sleep(500);
    }

    @AfterAll
    public static void tearDown() {
        // Topics are soft-deleted in @AfterEach. Hard delete after produce needs producer cache expiry;
        // the docker test profile resets cluster state between full E2E runs.
    }

    @AfterEach
    void softDeleteCreatedTopics() {
        for (String topicName : createdTopics) {
            makeDeleteRequest(
                getTopicsUri(project, topicName),
                ResourceDeletionType.SOFT_DELETE.toString(),
                EXPECTED_STATUS_204
            );
        }
        createdTopics.clear();
    }

    @Test
    public void enforcedTopic_sustainedOverQuota_returns429ThenRecovers() throws InterruptedException {
        String topicName = "rl_enforced_" + System.currentTimeMillis();
        createRateLimitTopic(topicName, RateLimiterMode.enforced, 1);

        // fallbackBuffer=0.25 on a single pod → ceil(1 × 1.25) = 2 qps tokens per window
        assertProduceStatus(topicName, 200);
        assertProduceStatus(topicName, 200);
        assertProduceThrottled(topicName);

        Thread.sleep(1_100);
        assertProduceStatus(topicName, 200);
    }

    @Test
    public void shadowTopic_overQuota_neverReturns429() throws InterruptedException {
        String topicName = "rl_shadow_" + System.currentTimeMillis();
        createRateLimitTopic(topicName, RateLimiterMode.shadow, 1);

        assertProduceStatus(topicName, 200);
        assertProduceStatus(topicName, 200);
        assertProduceStatus(topicName, 200);
    }

    @Test
    public void disabledTopic_overQuota_neverReturns429() throws InterruptedException {
        String topicName = "rl_disabled_" + System.currentTimeMillis();
        createRateLimitTopic(topicName, RateLimiterMode.disabled, 1);

        assertProduceStatus(topicName, 200);
        assertProduceStatus(topicName, 200);
        assertProduceStatus(topicName, 200);
    }

    @Test
    public void topicWithoutMode_neverReturns429() throws InterruptedException {
        String topicName = "rl_default_mode_" + System.currentTimeMillis();
        TopicResource topic = TopicResource.unGrouped(
            topicName,
            project.getName(),
            new TopicCapacityPolicy(1, 1024, 1, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "e2e"
        );
        makeCreateRequest(getTopicsUri(project), topic, EXPECTED_STATUS_OK);
        trackTopic(topicName);
        waitForTopicActivation();

        assertProduceStatus(topicName, 200);
        assertProduceStatus(topicName, 200);
    }

    private static void createRateLimitTopic(String topicName, RateLimiterMode mode, int qps)
        throws InterruptedException {
        TopicResource topic = TopicResource.unGrouped(
            topicName,
            project.getName(),
            new TopicCapacityPolicy(qps, 1024, 1, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "e2e"
        );
        topic.setRateLimiterMode(mode);
        topic.setPerRegionQuotaWeights(Map.of("default", 1.0));
        makeCreateRequest(getTopicsUri(project), topic, EXPECTED_STATUS_OK);
        trackTopic(topicName);
        waitForTopicActivation();
    }

    private static void trackTopic(String topicName) {
        createdTopics.add(topicName);
    }

    private static void waitForTopicActivation() throws InterruptedException {
        Thread.sleep(1_000);
    }

    private static void assertProduceStatus(String topicName, int expectedStatus) {
        try (Response response = postProduce(topicName)) {
            String raw = response.readEntity(String.class);
            Assertions.assertEquals(expectedStatus, response.getStatus(), raw);
        }
    }

    private static void assertProduceThrottled(String topicName) {
        try (Response response = postProduce(topicName)) {
            Assertions.assertEquals(429, response.getStatus());
            ErrorResponse error = response.readEntity(ErrorResponse.class);
            Assertions.assertEquals(RATE_LIMIT_MSG, error.reason());
        }
    }

    private static Response postProduce(String topicName) {
        String messageId = "e2e-rl-" + System.nanoTime();
        byte[] payload = "{\"e2e\":\"rate-limit\"}".getBytes();
        Map<String, String> headers = new HashMap<>();
        headers.put(HDR_MESSAGE_ID, messageId);
        return postProduceWithHeaders(getProduceUri(project, topicName), payload, headers);
    }
}
