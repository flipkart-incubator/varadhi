package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.web.QueueResource;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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
            null,
            Map.of("http://localhost:8080", "test"),
            null
        );
    }
}
