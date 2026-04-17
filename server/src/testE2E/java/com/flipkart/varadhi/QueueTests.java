package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.web.v1.admin.QueueHandlers;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_INCLUDE_INACTIVE;

public class QueueTests extends E2EBase {

    /** Server requires non-empty {@code targetClientIds} on queue subscription create/update. */
    private static final Map<String, String> DEFAULT_QUEUE_TARGET_CLIENT_IDS = Map.of(
        "http://localhost:8080/e2e-queue-consumer",
        "e2e-queue-test-client"
    );

    /** Server requires non-empty {@code properties} on queue subscription create/update. */
    private static final Map<String, String> DEFAULT_QUEUE_PROPERTIES = Map.of("key", "value");

    /**
     * Standard header names from {@code conf/configuration.yml} (queue produce requires message id, queue HTTP
     * target URI and method).
     */
    private static final String HDR_MESSAGE_ID = "X_MESSAGE_ID";
    private static final String HDR_HTTP_URI = "X_HTTP_URI";
    private static final String HDR_HTTP_METHOD = "X_HTTP_METHOD";

    private static Org org;
    private static Team team;
    private static Project project;
    private static boolean queueApiAvailable;

    @BeforeAll
    public static void setup() {
        org = Org.of("queue_org");
        team = Team.of("queue_team", org.getName());
        project = Project.of("queue_project", "", team.getName(), team.getOrg());

        makeCreateRequest(getOrgsUri(), org, 200);
        makeCreateRequest(getTeamsUri(team.getOrg()), team, 200);
        makeCreateRequest(getProjectCreateUri(), project, 200);

        try (Response response = makeHttpGetRequest(getQueuesUri(project))) {
            queueApiAvailable = response.getStatus() != 404;
        }
    }

    @AfterAll
    public static void tearDown() {
        cleanupOrgs(List.of(org));
    }

    @Test
    public void createAndDeleteQueue() {
        String queueName = "queue_create_delete";
        QueueResource queue = queueResource(queueName);

        try (Response createResponse = makeHttpPostRequest(getQueuesUri(project), queue)) {
            Assertions.assertEquals(200, createResponse.getStatus());
        }

        List<String> queues = getTopics(makeListRequest(getQueuesUri(project), 200));
        Assertions.assertTrue(queues.contains(queueName));

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);

        List<String> queuesAfterDelete = getTopics(makeListRequest(getQueuesUri(project), 200));
        Assertions.assertFalse(queuesAfterDelete.contains(queueName));
    }

    @Test
    public void createDuplicateQueue() {

        String queueName = "queue_duplicate";
        QueueResource queue = queueResource(queueName);

        try (Response createResponse = makeHttpPostRequest(getQueuesUri(project), queue)) {
            Assertions.assertEquals(200, createResponse.getStatus());
        }
        createQueueOk(queueName);

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void createQueueWithoutName() {

        QueueResource queue = queueResource(null);
        makeCreateRequest(getQueuesUri(project), queue, 400, "Invalid Queue name. Check naming constraints.", true);
    }

    @Test
    public void updateQueue_changesConsumptionPolicy() {

        String queueName = "queue_update_cp";
        createQueueOk(queueName);

        QueueHandlers.QueueResponse current = makeGetRequest(
            getQueuesUri(project, queueName),
            QueueHandlers.QueueResponse.class,
            200
        );
        QueueResource body = queueResourceFromGetResponse(current);
        ConsumptionPolicy cp = current.subscription().getConsumptionPolicy();
        ConsumptionPolicy newCp = new ConsumptionPolicy(
            cp.getMaxInFlightMessages(),
            cp.getMaxParallelism() + 3,
            cp.getMaxRecoveryAllocation(),
            cp.isDltRecoveryPreferred(),
            cp.getMaxErrorThreshold(),
            cp.getThrottlePolicy()
        );
        body.setConsumptionPolicy(newCp);

        QueueHandlers.QueueResponse updated;
        try (Response putResponse = makeHttpPutRequest(getQueuesUri(project, queueName), body)) {
            Assertions.assertEquals(200, putResponse.getStatus());
            updated = putResponse.readEntity(QueueHandlers.QueueResponse.class);
        }
        Assertions.assertEquals(
            newCp.getMaxParallelism(),
            updated.subscription().getConsumptionPolicy().getMaxParallelism()
        );

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void softDeleteAndRestoreQueue() {

        String queueName = "queue_soft_restore";
        createQueueOk(queueName);

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.SOFT_DELETE.toString(), 204);

        List<String> queues = getTopics(makeListRequest(getQueuesUri(project), 200));
        Assertions.assertFalse(queues.contains(queueName));

        String listInactiveUri = getQueuesUri(project) + "?" + QUERY_PARAM_INCLUDE_INACTIVE + "=true";
        queues = getTopics(makeListRequest(listInactiveUri, 200));
        Assertions.assertTrue(queues.contains(queueName));

        makePatchRequest(getQueuesUri(project, queueName) + "/restore", 204);

        QueueHandlers.QueueResponse restored = makeGetRequest(
            getQueuesUri(project, queueName),
            QueueHandlers.QueueResponse.class,
            200
        );
        Assertions.assertEquals(queueName, restored.name());

        queues = getTopics(makeListRequest(getQueuesUri(project), 200));
        Assertions.assertTrue(queues.contains(queueName));

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void createQueue_rejectedWhenPlainTopicExistsWithSameName() {

        String sharedName = "queue_plain_topic_conflict";
        TopicResource plainTopic = TopicResource.unGrouped(
            sharedName,
            project.getName(),
            null,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        makeCreateRequest(getTopicsUri(project), plainTopic, 200);

        String expectedReason =
            "Existing TOPIC %s has different values than in request for Topic Category. Current value: TOPIC. Value in Request: QUEUE.".formatted(
                sharedName
            );
        makeCreateRequest(getQueuesUri(project), queueResource(sharedName), 409, expectedReason, true);

        makeDeleteRequest(getTopicsUri(project, sharedName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    /**
     * If topic creation succeeded but default subscription creation failed (or subscription was removed), a retry of
     * POST queue should finish wiring the default subscription (see {@code VaradhiQueueService#create}).
     */
    @Test
    public void createQueue_retryCompletesWhenTopicExistsButDefaultSubscriptionMissing() {

        String queueName = "queue_retry_orphan_sub";
        createQueueOk(queueName);

        makeDeleteRequest(
            getSubscriptionsUri(project, QueueResource.getDefaultSubscriptionName(queueName)),
            ResourceDeletionType.HARD_DELETE.toString(),
            204
        );

        createQueueOk(queueName);
        QueueHandlers.QueueResponse got = makeGetRequest(
            getQueuesUri(project, queueName),
            QueueHandlers.QueueResponse.class,
            200
        );
        Assertions.assertEquals(queueName, got.name());

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void restoreQueue_whenAlreadyActive_isIdempotent204() {

        String queueName = "queue_restore_idempotent";
        createQueueOk(queueName);
        makePatchRequest(getQueuesUri(project, queueName) + "/restore", 204);
        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void restoreQueue_whenQueueNeverExisted_returns404() {

        String ghost = "queue_restore_never_existed";
        String expectedReason = "Cannot restore queue '%s': default subscription '%s' not found.".formatted(
            ghost,
            QueueResource.getDefaultSubscriptionName(ghost)
        );
        makePatchRequest(getQueuesUri(project, ghost) + "/restore", 404, expectedReason, true);
    }

    @Test
    public void restoreQueue_whenDefaultSubscriptionRemoved_returns404() {

        String queueName = "queue_restore_sub_removed";
        createQueueOk(queueName);
        makeDeleteRequest(
            getSubscriptionsUri(project, QueueResource.getDefaultSubscriptionName(queueName)),
            ResourceDeletionType.HARD_DELETE.toString(),
            204
        );

        String expectedReason = "Cannot restore queue '%s': default subscription '%s' not found.".formatted(
            queueName,
            QueueResource.getDefaultSubscriptionName(queueName)
        );
        makePatchRequest(getQueuesUri(project, queueName) + "/restore", 404, expectedReason, true);

        makeDeleteRequest(getTopicsUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void queueProduce_endToEnd_withRequiredHeaders_succeeds() {
        Assumptions.assumeTrue(queueApiAvailable, "Queue API not available in this environment");

        String queueName = "queue_e2e_produce_ok";
        createQueueOk(queueName);

        String messageId = "e2e-q-produce-" + System.currentTimeMillis();
        String callbackUri = DEFAULT_QUEUE_TARGET_CLIENT_IDS.keySet().iterator().next();
        byte[] payload = "{\"e2e\":\"queue-produce\"}".getBytes();

        Map<String, String> headers = new HashMap<>();
        headers.put(HDR_MESSAGE_ID, messageId);
        headers.put(HDR_HTTP_URI, callbackUri);
        headers.put(HDR_HTTP_METHOD, "POST");
        headers.put("X_TRACE_E2E", "queue-produce-trace");

        String produceUri = getProduceUri(project, queueName);
        try (Response response = postProduceWithHeaders(produceUri, payload, headers)) {
            int status = response.getStatus();
            String raw = response.readEntity(String.class);
            Assertions.assertEquals(200, status, raw);
            String returnedId = JsonMapper.jsonDeserialize(raw, String.class);
            Assertions.assertEquals(messageId, returnedId);
        }

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void queueProduce_endToEnd_missingHttpMethod_returnsBadRequest() {
        Assumptions.assumeTrue(queueApiAvailable, "Queue API not available in this environment");

        String queueName = "queue_e2e_produce_bad_hdr";
        createQueueOk(queueName);

        String messageId = "e2e-q-produce-bad-" + System.currentTimeMillis();
        String callbackUri = DEFAULT_QUEUE_TARGET_CLIENT_IDS.keySet().iterator().next();

        Map<String, String> headers = new HashMap<>();
        headers.put(HDR_MESSAGE_ID, messageId);
        headers.put(HDR_HTTP_URI, callbackUri);

        String produceUri = getProduceUri(project, queueName);
        try (Response response = postProduceWithHeaders(
            produceUri,
            "{\"e2e\":\"missing-method\"}".getBytes(),
            headers
        )) {
            Assertions.assertEquals(400, response.getStatus());
            ErrorResponse err = response.readEntity(ErrorResponse.class);
            Assertions.assertTrue(
                err.reason().contains("Missing required header " + HDR_HTTP_METHOD + " for queue produce"),
                err.reason()
            );
        }

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void queueProduce_endToEnd_missingMessageId_returnsBadRequest() {
        Assumptions.assumeTrue(queueApiAvailable, "Queue API not available in this environment");

        String queueName = "queue_e2e_produce_no_msgid";
        createQueueOk(queueName);

        String callbackUri = DEFAULT_QUEUE_TARGET_CLIENT_IDS.keySet().iterator().next();
        Map<String, String> headers = new HashMap<>();
        headers.put(HDR_HTTP_URI, callbackUri);
        headers.put(HDR_HTTP_METHOD, "POST");

        String produceUri = getProduceUri(project, queueName);
        try (Response response = postProduceWithHeaders(
            produceUri,
            "{\"e2e\":\"no-msg-id\"}".getBytes(),
            headers
        )) {
            Assertions.assertEquals(400, response.getStatus());
            ErrorResponse err = response.readEntity(ErrorResponse.class);
            Assertions.assertTrue(
                err.reason().contains("Missing required header " + HDR_MESSAGE_ID + " for queue produce"),
                err.reason()
            );
        }

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void restoreQueue_afterFullHardDelete_returns404() {

        String queueName = "queue_restore_after_hard_delete";
        createQueueOk(queueName);
        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);

        String expectedReason = "Cannot restore queue '%s': default subscription '%s' not found.".formatted(
            queueName,
            QueueResource.getDefaultSubscriptionName(queueName)
        );
        makePatchRequest(getQueuesUri(project, queueName) + "/restore", 404, expectedReason, true);
    }

    @Test
    public void updateQueue_whenNotFound_returns404() {

        String missing = "queue_update_missing";
        String expectedReason = "Cannot update queue '%s': default subscription '%s' not found.".formatted(
            missing,
            QueueResource.getDefaultSubscriptionName(missing)
        );
        try (Response r = makeHttpPutRequest(getQueuesUri(project, missing), queueResource(missing))) {
            Assertions.assertEquals(404, r.getStatus());
            String reason = r.readEntity(ErrorResponse.class).reason();
            Assertions.assertTrue(
                expectedReason.equals(reason) || reason.equals("PROJECT(%s) not found".formatted(project.getName())),
                reason
            );
        }
    }

    @Test
    public void updateQueue_updatesTargetClientIds() {

        String queueName = "queue_update_client_ids";
        createQueueOk(queueName);

        QueueHandlers.QueueResponse current = makeGetRequest(
            getQueuesUri(project, queueName),
            QueueHandlers.QueueResponse.class,
            200
        );
        QueueResource body = queueResourceFromGetResponse(current);
        Map<String, String> newTargets = new HashMap<>();
        newTargets.put("http://localhost:9090", "other-client");
        body.setTargetClientIds(newTargets);

        QueueHandlers.QueueResponse updated;
        try (Response putResponse = makeHttpPutRequest(getQueuesUri(project, queueName), body)) {
            Assertions.assertEquals(200, putResponse.getStatus());
            updated = putResponse.readEntity(QueueHandlers.QueueResponse.class);
        }
        Assertions.assertEquals(newTargets, updated.subscription().getTargetClientIds());

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void updateQueue_updatesRetryPolicy() {

        String queueName = "queue_update_retry";
        createQueueOk(queueName);

        QueueHandlers.QueueResponse current = makeGetRequest(
            getQueuesUri(project, queueName),
            QueueHandlers.QueueResponse.class,
            200
        );
        QueueResource body = queueResourceFromGetResponse(current);
        RetryPolicy rp = current.subscription().getRetryPolicy();
        RetryPolicy newRp = new RetryPolicy(
            rp.getRetryCodes(),
            rp.getBackoffType(),
            rp.getMinBackoff(),
            rp.getMaxBackoff(),
            rp.getMultiplier(),
            rp.getRetryAttempts() + 2
        );
        body.setRetryPolicy(newRp);

        QueueHandlers.QueueResponse updated;
        try (Response putResponse = makeHttpPutRequest(getQueuesUri(project, queueName), body)) {
            Assertions.assertEquals(200, putResponse.getStatus());
            updated = putResponse.readEntity(QueueHandlers.QueueResponse.class);
        }
        Assertions.assertEquals(newRp.getRetryAttempts(), updated.subscription().getRetryPolicy().getRetryAttempts());

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    /**
     * POST create returns {@link QueueHandlers.QueueResponse} JSON (topic/subscription envelope), not {@link QueueResource},
     * so assert status only instead of {@link E2EBase#makeCreateRequest} which would deserialize the body as {@code QueueResource}.
     */
    private static void createQueueOk(String queueName) {
        try (Response r = makeHttpPostRequest(getQueuesUri(project), queueResource(queueName))) {
            Assertions.assertEquals(200, r.getStatus());
        }
    }

    private static QueueResource queueResource(String queueName) {
        QueueResource queue = new QueueResource(queueName, 0, project.getName());
        queue.setTargetClientIds(new HashMap<>(DEFAULT_QUEUE_TARGET_CLIENT_IDS));
        queue.setProperties(new HashMap<>(DEFAULT_QUEUE_PROPERTIES));
        return queue;
    }

    /**
     * Fills subscription defaults only when missing or empty, so GET-based PUT bodies stay valid without overwriting
     * server state when present.
     */
    private static void applyQueueDefaultsIfUnset(QueueResource queue) {
        if (queue.getTargetClientIds() == null || queue.getTargetClientIds().isEmpty()) {
            queue.setTargetClientIds(new HashMap<>(DEFAULT_QUEUE_TARGET_CLIENT_IDS));
        }
        if (queue.getProperties() == null || queue.getProperties().isEmpty()) {
            queue.setProperties(new HashMap<>(DEFAULT_QUEUE_PROPERTIES));
        }
    }

    /**
     * Builds a PUT body from GET queue response; {@code version} must match the default subscription for optimistic locking.
     */
    private static QueueResource queueResourceFromGetResponse(QueueHandlers.QueueResponse qr) {
        TopicResource t = qr.topic();
        SubscriptionResource s = qr.subscription();
        Map<String, String> targetClientIds = s.getTargetClientIds() != null ?
            new HashMap<>(s.getTargetClientIds()) :
            null;
        Map<String, String> properties = s.getProperties() != null ? new HashMap<>(s.getProperties()) : null;
        QueueResource queue = new QueueResource(
            qr.name(),
            s.getVersion(),
            qr.project(),
            t.isSecured(),
            t.isGrouped(),
            t.getAppId(),
            t.getNfrStrategy(),
            null,
            t.getCapacity(),
            t.getActionCode(),
            t.getNfrFilterName(),
            s.getRetryPolicy(),
            s.getConsumptionPolicy(),
            s.getCallbackConfig(),
            targetClientIds,
            properties
        );
        applyQueueDefaultsIfUnset(queue);
        return queue;
    }
}
