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
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * VIP-0001 §16 produce-path E2E against a running server ({@code rateLimiterOptions.enabled=true} in
 * docker config). HTTP/2 streaming and multi-pod even-split scenarios are covered at integration level.
 */
public class RateLimitProduceE2ETest extends E2EBase {

    private static final String HDR_MESSAGE_ID = "X_MESSAGE_ID";
    private static final String RATE_LIMIT_MSG =
        "Produce to Topic/Queue is currently rate limited, try again after sometime.";

    private static final long POLL_INTERVAL_MS = 100;
    private static final long POLL_TIMEOUT_MS = 10_000;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_UNPROCESSABLE = 422;
    private static final int HTTP_OK = 200;
    private static final int HTTP_RATE_LIMITED = 429;

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
        waitUntilProjectReadable();
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

        int admits = drainBurstUntilThrottled(topicName);
        Assertions.assertTrue(admits >= 1, "expected at least one admit before throttle");
        assertProduceThrottled(topicName);

        waitForProduceStatus(topicName, HTTP_OK);
    }

    @Test
    public void shadowTopic_overQuota_neverReturns429() throws InterruptedException {
        String topicName = "rl_shadow_" + System.currentTimeMillis();
        createRateLimitTopic(topicName, RateLimiterMode.shadow, 1);

        assertProduceStatus(topicName, HTTP_OK);
        assertProduceStatus(topicName, HTTP_OK);
        assertProduceStatus(topicName, HTTP_OK);
    }

    @Test
    public void disabledTopic_overQuota_neverReturns429() throws InterruptedException {
        String topicName = "rl_disabled_" + System.currentTimeMillis();
        createRateLimitTopic(topicName, RateLimiterMode.disabled, 1);

        assertProduceStatus(topicName, HTTP_OK);
        assertProduceStatus(topicName, HTTP_OK);
        assertProduceStatus(topicName, HTTP_OK);
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
        waitForProducePathReady(topicName);

        assertProduceStatus(topicName, HTTP_OK);
        assertProduceStatus(topicName, HTTP_OK);
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
        waitForProducePathReady(topicName);
    }

    private static void trackTopic(String topicName) {
        createdTopics.add(topicName);
    }

    private static void waitUntilProjectReadable() throws InterruptedException {
        pollUntil("project " + project.getName() + " readable", () -> {
            try {
                Project fetched = makeGetRequest(getProjectUri(project), Project.class, EXPECTED_STATUS_OK);
                return project.getName().equals(fetched.getName());
            } catch (AssertionError ignored) {
                return false;
            }
        });
    }

    /**
     * Polls produce until the topic is on the hot path (not still provisioning). {@code 404} means the
     * topic is missing or inactive; {@code 422} means lifecycle/storage is not yet {@code Producing}.
     * A {@code 429} during this wait does not debit buckets (reject is check-only), so it is a valid
     * readiness signal without spending burst budget.
     */
    private static void waitForProducePathReady(String topicName) throws InterruptedException {
        pollUntil("produce path ready for " + topicName, () -> {
            int status = produceStatus(topicName);
            return status != HTTP_NOT_FOUND && status != HTTP_UNPROCESSABLE;
        });
    }

    /** Produces until the first non-200 (expected {@code 429}); returns how many messages were admitted. */
    private static int drainBurstUntilThrottled(String topicName) {
        int admits = 0;
        int status;
        do {
            status = produceStatus(topicName);
            if (status == HTTP_OK) {
                admits++;
            }
        } while (status == HTTP_OK);
        Assertions.assertEquals(HTTP_RATE_LIMITED, status, "expected throttle after burst exhausted");
        return admits;
    }

    private static void waitForProduceStatus(String topicName, int expectedStatus) throws InterruptedException {
        pollUntil(
            "produce to " + topicName + " returns " + expectedStatus,
            () -> produceStatus(topicName) == expectedStatus
        );
    }

    private static int produceStatus(String topicName) {
        try (Response response = postProduce(topicName)) {
            return response.getStatus();
        }
    }

    private static void pollUntil(String description, BooleanSupplier condition) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(POLL_TIMEOUT_MS);
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        Assertions.assertTrue(condition.getAsBoolean(), description + " within " + POLL_TIMEOUT_MS + "ms");
    }

    private static void assertProduceStatus(String topicName, int expectedStatus) {
        try (Response response = postProduce(topicName)) {
            String raw = response.readEntity(String.class);
            Assertions.assertEquals(expectedStatus, response.getStatus(), raw);
        }
    }

    private static void assertProduceThrottled(String topicName) {
        try (Response response = postProduce(topicName)) {
            Assertions.assertEquals(HTTP_RATE_LIMITED, response.getStatus());
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
