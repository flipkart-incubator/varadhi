package com.flipkart.varadhi.web.v1.admin;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.core.VaradhiQueueService;
import com.flipkart.varadhi.entities.CodeRange;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.SubscriptionTestUtils;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.configurators.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.routes.TelemetryType;
import io.opentelemetry.api.common.AttributeKey;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import lombok.experimental.ExtensionMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
class QueueHandlersTest extends WebTestBase {

    private static final String QUEUE_NAME = "queue1";
    private static final String TEAM_NAME = "team1";
    private static final String ORG_NAME = "org1";
    private static final String DEFAULT_PROJECT_NAME = "project1";

    private static final String TARGET_CLIENT_KEY = "http://localhost:8080";

    private final Project project = Project.of(DEFAULT_PROJECT_NAME, "", TEAM_NAME, ORG_NAME);

    private QueueHandlers queueHandlers;

    @Mock
    private VaradhiQueueService varadhiQueueService;

    private RequestTelemetryConfigurator requestTelemetryConfigurator;

    private AutoCloseable openMocksHandle;

    @BeforeEach
    void preTest() throws InterruptedException {
        openMocksHandle = MockitoAnnotations.openMocks(this);
        super.setUp();
        requestTelemetryConfigurator = RequestTelemetryConfigurator.getDefault(spanProvider);
        Resource.EntityResource<Project> projectResource = Resource.of(project, ResourceType.PROJECT);
        doReturn(projectResource).when(projectCache).getOrThrow(any());

        queueHandlers = new QueueHandlers(varadhiQueueService, projectCache);
        setupRoutes();
    }

    private void setupRoutes() {
        router.post("/v1/projects/:project/queues").handler(bodyHandler).handler(ctx -> {
            queueHandlers.setRequestBody(ctx);
            ctx.next();
        }).handler(ctx -> {
            requestTelemetryConfigurator.addRequestSpanAndLog(ctx, "CreateQueue", TelemetryType.ALL);
            ctx.next();
        }).handler(wrapBlocking(queueHandlers::create));

        router.get("/v1/projects/:project/queues/:queue").handler(wrapBlocking(queueHandlers::get));

        router.get("/v1/projects/:project/queues").handler(bodyHandler).handler(wrapBlocking(queueHandlers::list));

        // Async handlers (handleResponse); must not use wrapBlocking — same as production nonBlocking routes.
        Route deleteRoute = router.delete("/v1/projects/:project/queues/:queue").handler(queueHandlers::delete);
        Route restoreRoute = router.patch("/v1/projects/:project/queues/:queue/restore")
                                   .handler(queueHandlers::restore);
        Route updateRoute = router.put("/v1/projects/:project/queues/:queue").handler(bodyHandler).handler(ctx -> {
            queueHandlers.setRequestBody(ctx);
            ctx.next();
        }).handler(queueHandlers::update);
        setupFailureHandler(deleteRoute);
        setupFailureHandler(restoreRoute);
        setupFailureHandler(updateRoute);

        router.getRoutes().forEach(r -> {
            if (r != deleteRoute && r != restoreRoute && r != updateRoute) {
                setupFailureHandler(r);
            }
        });
    }

    @AfterEach
    void postTest() throws Exception {
        if (openMocksHandle != null) {
            openMocksHandle.close();
        }
        super.tearDown();
    }

    @Test
    void createQueue_withValidRequest_createsQueue() throws InterruptedException {
        QueueResource body = sampleQueueResource();
        VaradhiTopic topic = topicForQueue();
        VaradhiSubscription subscription = subscriptionForQueue(topic);

        when(varadhiQueueService.create(any(), eq(project), any())).thenReturn(
            new VaradhiQueueService.QueueResult(topic, subscription)
        );

        QueueHandlers.QueueResponse created = sendRequestWithEntity(
            createRequest(HttpMethod.POST, queuesUrl(project)),
            body,
            WebTestBase.c(QueueHandlers.QueueResponse.class)
        );

        assertEquals(QUEUE_NAME, created.queueName());
        assertEquals(project.getName(), created.project());
        verify(varadhiQueueService).create(any(), eq(project), any());
        var spans = spanExporter.getFinishedSpanItems();
        Assertions.assertEquals(1, spans.size());
        Assertions.assertEquals("CreateQueue", spans.get(0).getAttributes().get(AttributeKey.stringKey("api")));
    }

    /**
     * Missing queue name is rejected by {@link com.flipkart.varadhi.entities.web.QueueResource} bean validation
     * ({@link com.flipkart.varadhi.entities.ValidateResource}) before {@link VaradhiQueueService#create} runs.
     * E2E may assert {@code "Queue name is required."} when that layer is skipped or ordered differently.
     */
    @Test
    void createQueue_withoutName_returns400() throws InterruptedException {
        QueueResource body = sampleQueueResource();
        body.setName(null);

        sendRequestWithEntity(
            createRequest(HttpMethod.POST, queuesUrl(project)),
            body,
            HTTP_BAD_REQUEST,
            "Invalid Queue name. Check naming constraints.",
            WebTestBase.c(ErrorResponse.class)
        );

        verify(varadhiQueueService, never()).create(any(), any(), any());
    }

    @Test
    void createQueue_blankNameAfterTrim_returns400() throws InterruptedException {
        QueueResource body = sampleQueueResource();
        body.setName("  \t  ");

        sendRequestWithEntity(
            createRequest(HttpMethod.POST, queuesUrl(project)),
            body,
            HTTP_BAD_REQUEST,
            "Invalid Queue name. Check naming constraints.",
            WebTestBase.c(ErrorResponse.class)
        );

        verify(varadhiQueueService, never()).create(any(), any(), any());
    }

    @Test
    void getQueue_withValidRequest_returnsTopicAndSubscription() throws InterruptedException {
        VaradhiTopic topic = topicForQueue();
        VaradhiSubscription subscription = subscriptionForQueue(topic);
        when(varadhiQueueService.get(project.getName(), QUEUE_NAME)).thenReturn(
            new VaradhiQueueService.QueueResult(topic, subscription)
        );

        QueueHandlers.QueueResponse response = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, queueUrl(project)),
            WebTestBase.c(QueueHandlers.QueueResponse.class)
        );

        assertEquals(QUEUE_NAME, response.queueName());
        assertEquals(project.getName(), response.project());
        assertEquals(QUEUE_NAME, response.topic().getName());
        verify(varadhiQueueService).get(project.getName(), QUEUE_NAME);
    }

    @Test
    void listQueues_whenQueuesExist_returnsNames() throws InterruptedException {
        List<String> names = List.of("q1", "q2");
        when(varadhiQueueService.list(project.getName(), false)).thenReturn(names);

        List<String> out = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, queuesUrl(project)),
            WebTestBase.c(List.class)
        );

        assertEquals(2, out.size());
        verify(varadhiQueueService).list(project.getName(), false);
    }

    @Test
    void listQueues_withIncludeInactive_passesTrue() throws InterruptedException {
        when(varadhiQueueService.list(project.getName(), true)).thenReturn(Collections.emptyList());

        sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, queuesUrl(project) + "?includeInactive=true"),
            WebTestBase.c(List.class)
        );

        verify(varadhiQueueService).list(project.getName(), true);
    }

    @Test
    void listQueues_whenEmpty_returnsEmptyList() throws InterruptedException {
        when(varadhiQueueService.list(project.getName(), false)).thenReturn(List.of());

        List<String> out = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, queuesUrl(project)),
            WebTestBase.c(List.class)
        );

        assertTrue(out.isEmpty());
    }

    @Test
    void deleteQueue_softDelete_thenDeletesTopicAfterSubscription() throws InterruptedException {
        verifyDelete(ResourceDeletionType.SOFT_DELETE, "SOFT_DELETE");
    }

    @Test
    void deleteQueue_hardDelete_thenDeletesTopicAfterSubscription() throws InterruptedException {
        verifyDelete(ResourceDeletionType.HARD_DELETE, "HARD_DELETE");
    }

    @Test
    void deleteQueue_noDeletionType_defaultsToSoftDelete() throws InterruptedException {
        verifyDelete(ResourceDeletionType.SOFT_DELETE, null);
    }

    @Test
    void updateQueue_withValidRequest_returnsQueueResponse() throws InterruptedException {
        QueueResource body = sampleQueueResource();
        body.setVersion(2);
        VaradhiTopic topic = topicForQueue();
        VaradhiSubscription updated = subscriptionForQueue(topic);
        updated.setVersion(3);

        when(varadhiQueueService.updateQueue(eq(DEFAULT_PROJECT_NAME), eq(QUEUE_NAME), any(), any(), any())).thenReturn(
            CompletableFuture.completedFuture(new VaradhiQueueService.QueueResult(topic, updated))
        );

        QueueHandlers.QueueResponse resp = sendRequestWithEntity(
            createRequest(HttpMethod.PUT, queueUrl(project)),
            body,
            WebTestBase.c(QueueHandlers.QueueResponse.class)
        );

        assertEquals(QUEUE_NAME, resp.queueName());
        assertEquals(DEFAULT_PROJECT_NAME, resp.project());
        assertEquals(SubscriptionResource.from(updated).getRetryPolicy(), resp.subscription().getRetryPolicy());
        verify(varadhiQueueService).updateQueue(eq(DEFAULT_PROJECT_NAME), eq(QUEUE_NAME), any(), any(), any());
    }

    @Test
    void updateQueue_projectMismatch_returns400() throws InterruptedException {
        QueueResource body = sampleQueueResource();
        body.setProject("wrong-project");

        sendRequestWithEntity(
            createRequest(HttpMethod.PUT, queueUrl(project)),
            body,
            HTTP_BAD_REQUEST,
            "Project name mismatch between URL and request body.",
            WebTestBase.c(ErrorResponse.class)
        );
    }

    @Test
    void updateQueue_queueNameMismatch_returns400() throws InterruptedException {
        QueueResource body = sampleQueueResource();
        body.setName("other-queue");

        sendRequestWithEntity(
            createRequest(HttpMethod.PUT, queueUrl(project)),
            body,
            HTTP_BAD_REQUEST,
            "Queue name in path must match request body name.",
            WebTestBase.c(ErrorResponse.class)
        );
    }

    @Test
    void listQueues_pathProjectForwardedToService() throws InterruptedException {
        String altProject = "alt-project";
        Project altProjectEntity = Project.of(altProject, "", TEAM_NAME, ORG_NAME);

        when(varadhiQueueService.list(altProject, false)).thenReturn(List.of("q-a"));

        @SuppressWarnings ("unchecked")
        List<String> out = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, queuesUrl(altProjectEntity)),
            WebTestBase.c(List.class)
        );

        assertEquals(1, out.size());
        assertEquals("q-a", out.get(0));
        verify(varadhiQueueService).list(altProject, false);
    }

    @Test
    void getQueue_pathProjectForwardedToService() throws InterruptedException {
        String altProject = "alt-project";
        Project alt = Project.of(altProject, "", TEAM_NAME, ORG_NAME);

        VaradhiTopic topic = topicForQueueOnProject(alt);
        VaradhiSubscription subscription = subscriptionForQueueOnProject(alt, topic);
        when(varadhiQueueService.get(altProject, QUEUE_NAME)).thenReturn(
            new VaradhiQueueService.QueueResult(topic, subscription)
        );

        QueueHandlers.QueueResponse response = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, String.join("/", "/v1/projects", altProject, "queues", QUEUE_NAME)),
            WebTestBase.c(QueueHandlers.QueueResponse.class)
        );

        assertEquals(QUEUE_NAME, response.queueName());
        assertEquals(altProject, response.project());
        verify(varadhiQueueService).get(altProject, QUEUE_NAME);
    }

    @Test
    void getQueue_whenServiceThrowsNotFound_returns404() throws InterruptedException {
        String msg = "QUEUE(project1/queue1) not found";
        doThrow(new ResourceNotFoundException(msg)).when(varadhiQueueService).get(project.getName(), QUEUE_NAME);

        sendRequestWithoutPayload(createRequest(HttpMethod.GET, queueUrl(project)), HTTP_NOT_FOUND, msg);
    }

    @Test
    void createQueue_whenServiceThrowsInvalidOperation_returns409() throws InterruptedException {
        String msg = "Cannot create queue: conflict.";
        doThrow(new InvalidOperationForResourceException(msg)).when(varadhiQueueService)
                                                              .create(any(), eq(project), any());

        sendRequestWithEntity(
            createRequest(HttpMethod.POST, queuesUrl(project)),
            sampleQueueResource(),
            HTTP_CONFLICT,
            msg,
            WebTestBase.c(ErrorResponse.class)
        );
    }

    @Test
    void createQueue_passesRetryPolicyAndTargetClientIdsToService() throws InterruptedException {
        RetryPolicy customRetry = new RetryPolicy(
            new CodeRange[] {new CodeRange(503, 503)},
            RetryPolicy.BackoffType.EXPONENTIAL,
            2,
            2,
            2,
            5
        );
        Map<String, String> clients = Map.of("http://callback.example/push", "client-b");
        QueueResource body = new QueueResource(
            QUEUE_NAME,
            0,
            DEFAULT_PROJECT_NAME,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            customRetry,
            com.flipkart.varadhi.entities.Constants.QueueDefaults.CONSUMPTION_POLICY,
            null,
            clients,
            SubscriptionTestUtils.getSubscriptionDefaultProperties()
        );
        VaradhiTopic topic = topicForQueue();
        VaradhiSubscription subscription = subscriptionForQueue(topic);
        when(varadhiQueueService.create(any(), eq(project), any())).thenReturn(
            new VaradhiQueueService.QueueResult(topic, subscription)
        );

        sendRequestWithEntity(
            createRequest(HttpMethod.POST, queuesUrl(project)),
            body,
            WebTestBase.c(QueueHandlers.QueueResponse.class)
        );

        ArgumentCaptor<QueueResource> cap = ArgumentCaptor.forClass(QueueResource.class);
        verify(varadhiQueueService).create(cap.capture(), eq(project), any());
        assertEquals(customRetry, cap.getValue().getRetryPolicy());
        assertEquals("client-b", cap.getValue().getTargetClientIds().get("http://callback.example/push"));
    }

    @Test
    void restoreQueue_afterSubscriptionFuture_runsTopicRestore() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PATCH, queueUrl(project) + "/restore");
        doReturn(CompletableFuture.completedFuture(null)).when(varadhiQueueService)
                                                         .restoreQueue(
                                                             eq(project.getName()),
                                                             eq(QUEUE_NAME),
                                                             any(),
                                                             any()
                                                         );

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(varadhiQueueService, times(1)).restoreQueue(eq(project.getName()), eq(QUEUE_NAME), any(), any());
    }

    @Test
    void restoreQueue_whenServiceReturnsFailedFuture_returns404() throws InterruptedException {
        String msg = "Cannot restore queue 'queue1': default subscription not found.";
        HttpRequest<Buffer> request = createRequest(HttpMethod.PATCH, queueUrl(project) + "/restore");
        doReturn(CompletableFuture.failedFuture(new ResourceNotFoundException(msg))).when(varadhiQueueService)
                                                                                    .restoreQueue(
                                                                                        eq(project.getName()),
                                                                                        eq(QUEUE_NAME),
                                                                                        any(),
                                                                                        any()
                                                                                    );

        sendRequestWithoutPayload(request, HTTP_NOT_FOUND, msg);
    }

    @Test
    void deleteQueue_whenServiceReturnsFailedFuture_returns404() throws InterruptedException {
        String msg = "SUBSCRIPTION(project1/sub_queue1) not found";
        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, queueUrl(project));
        doReturn(CompletableFuture.failedFuture(new ResourceNotFoundException(msg))).when(varadhiQueueService)
                                                                                    .deleteQueue(
                                                                                        eq(project.getName()),
                                                                                        eq(QUEUE_NAME),
                                                                                        eq(project),
                                                                                        any(),
                                                                                        eq(
                                                                                            ResourceDeletionType.SOFT_DELETE
                                                                                        ),
                                                                                        any()
                                                                                    );

        sendRequestWithoutPayload(request, HTTP_NOT_FOUND, msg);
    }

    private void verifyDelete(ResourceDeletionType expectedType, String queryDeletionType) throws InterruptedException {
        String url = queueUrl(project);
        if (queryDeletionType != null) {
            url += "?deletionType=" + queryDeletionType;
        }

        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, url);
        doReturn(CompletableFuture.completedFuture(null)).when(varadhiQueueService)
                                                         .deleteQueue(
                                                             eq(project.getName()),
                                                             eq(QUEUE_NAME),
                                                             eq(project),
                                                             any(),
                                                             eq(expectedType),
                                                             any()
                                                         );

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(varadhiQueueService, times(1)).deleteQueue(
            eq(project.getName()),
            eq(QUEUE_NAME),
            eq(project),
            any(),
            eq(expectedType),
            any()
        );
    }

    private static QueueResource sampleQueueResource() {
        return new QueueResource(
            QUEUE_NAME,
            0,
            DEFAULT_PROJECT_NAME,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            com.flipkart.varadhi.entities.Constants.QueueDefaults.RETRY_POLICY,
            com.flipkart.varadhi.entities.Constants.QueueDefaults.CONSUMPTION_POLICY,
            null,
            Map.of(TARGET_CLIENT_KEY, "test"),
            SubscriptionTestUtils.getSubscriptionDefaultProperties()
        );
    }

    private VaradhiTopic topicForQueue() {
        return topicForQueueOnProject(project);
    }

    private static VaradhiTopic topicForQueueOnProject(Project p) {
        TopicResource tr = TopicResource.grouped(
            QUEUE_NAME,
            p.getName(),
            Constants.DEFAULT_TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.USER_ACTION,
            "test"
        );
        return tr.toVaradhiTopic();
    }

    private VaradhiSubscription subscriptionForQueue(VaradhiTopic topic) {
        return subscriptionForQueueOnProject(project, topic);
    }

    private static VaradhiSubscription subscriptionForQueueOnProject(Project project, VaradhiTopic topic) {
        return SubscriptionTestUtils.createUngroupedSubscription(
            QueueResource.getDefaultSubscriptionName(QUEUE_NAME),
            project,
            topic
        );
    }

    private static String queuesUrl(Project project) {
        return String.join("/", "/v1/projects", project.getName(), "queues");
    }

    private static String queueUrl(Project project) {
        return String.join("/", queuesUrl(project), QUEUE_NAME);
    }
}
