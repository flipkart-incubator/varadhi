package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.web.v1.admin.QueueHandlers;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_INCLUDE_INACTIVE;

public class QueueTests extends E2EBase {

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
        Assumptions.assumeTrue(queueApiAvailable, "Queue API is not available in this server image.");
        String queueName = "queue_create_delete";
        QueueResource queue = queueResource(queueName);

        try (Response createResponse = makeHttpPostRequest(getQueuesUri(project), queue)) {
            Assertions.assertEquals(200, createResponse.getStatus());
        }
        makeCreateRequest(getQueuesUri(project), queue, 409, null, true);

        List<String> queues = getTopics(makeListRequest(getQueuesUri(project), 200));
        Assertions.assertTrue(queues.contains(queueName));

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);

        List<String> queuesAfterDelete = getTopics(makeListRequest(getQueuesUri(project), 200));
        Assertions.assertFalse(queuesAfterDelete.contains(queueName));
    }

    @Test
    public void createDuplicateQueue() {
        Assumptions.assumeTrue(queueApiAvailable, "Queue API is not available in this server image.");
        String queueName = "queue_duplicate";
        QueueResource queue = queueResource(queueName);

        try (Response createResponse = makeHttpPostRequest(getQueuesUri(project), queue)) {
            Assertions.assertEquals(200, createResponse.getStatus());
        }
        makeCreateRequest(getQueuesUri(project), queue, 409, null, true);

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    @Test
    public void createQueueWithoutName() {
        Assumptions.assumeTrue(queueApiAvailable, "Queue API is not available in this server image.");
        QueueResource queue = queueResource(null);
        makeCreateRequest(getQueuesUri(project), queue, 400, "Queue name is required.", true);
    }

    @Test
    public void updateQueue_changesConsumptionPolicy() {
        Assumptions.assumeTrue(queueApiAvailable, "Queue API is not available in this server image.");
        String queueName = "queue_update_cp";
        makeCreateRequest(getQueuesUri(project), queueResource(queueName), 200);

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
        Assumptions.assumeTrue(queueApiAvailable, "Queue API is not available in this server image.");
        String queueName = "queue_soft_restore";
        makeCreateRequest(getQueuesUri(project), queueResource(queueName), 200);

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
        Assertions.assertEquals(queueName, restored.queueName());

        queues = getTopics(makeListRequest(getQueuesUri(project), 200));
        Assertions.assertTrue(queues.contains(queueName));

        makeDeleteRequest(getQueuesUri(project, queueName), ResourceDeletionType.HARD_DELETE.toString(), 204);
    }

    private static QueueResource queueResource(String queueName) {
        return new QueueResource(
            queueName,
            0,
            project.getName(),
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Map.of("http://localhost:8080", "test"),
            null
        );
    }

    /**
     * Builds a PUT body from GET queue response; {@code version} must match the default subscription for optimistic locking.
     */
    private static QueueResource queueResourceFromGetResponse(QueueHandlers.QueueResponse qr) {
        TopicResource t = qr.topic();
        SubscriptionResource s = qr.subscription();
        return new QueueResource(
            qr.queueName(),
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
            s.getTargetClientIds(),
            s.getProperties()
        );
    }
}
