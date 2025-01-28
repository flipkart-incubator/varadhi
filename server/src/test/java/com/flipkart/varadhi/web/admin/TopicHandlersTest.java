package com.flipkart.varadhi.web.admin;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.utils.JsonMapper;
import com.flipkart.varadhi.utils.VaradhiTopicFactory;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.entities.TopicResource;
import com.flipkart.varadhi.web.routes.TelemetryType;
import com.flipkart.varadhi.web.v1.admin.TopicHandlers;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;

import static com.flipkart.varadhi.web.RequestTelemetryConfigurator.REQUEST_SPAN_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;

class TopicHandlersTest extends WebTestBase {

    private static final String TOPIC_NAME = "topic1";
    private static final String TEAM_NAME = "team1";
    private static final String ORG_NAME = "org1";
    private static final String DEFAULT_PROJECT_NAME = "project1";

    private final Project project = Project.of(DEFAULT_PROJECT_NAME, "", TEAM_NAME, ORG_NAME);

    @InjectMocks
    private TopicHandlers topicHandlers;

    @Mock
    private VaradhiTopicService varadhiTopicService;

    @Mock
    private VaradhiTopicFactory varadhiTopicFactory;

    @Mock
    private ProjectService projectService;

    @Mock
    private SpanProvider spanProvider;

    @Mock
    private Span span;

    private RequestTelemetryConfigurator requestTelemetryConfigurator;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
        doReturn(span).when(spanProvider).addSpan(REQUEST_SPAN_NAME);
        requestTelemetryConfigurator = new RequestTelemetryConfigurator(spanProvider, new SimpleMeterRegistry());
        doReturn(project).when(projectService).getCachedProject(project.getName());

        setupRoutes();
    }

    private void setupRoutes() {
        router.post("/projects/:project/topics")
                .handler(bodyHandler)
                .handler(ctx -> {
                    topicHandlers.setRequestBody(ctx);
                    ctx.next();
                })
                .handler(ctx -> {
                    requestTelemetryConfigurator.addRequestSpanAndLog(ctx, "CreateTopic", TelemetryType.ALL);
                    ctx.next();
                })
                .handler(wrapBlocking(topicHandlers::create));

        router.get("/projects/:project/topics/:topic")
                .handler(wrapBlocking(topicHandlers::get));

        router.get("/projects/:project/topics")
                .handler(bodyHandler)
                .handler(wrapBlocking(topicHandlers::listTopics));

        router.delete("/projects/:project/topics/:topic")
                .handler(wrapBlocking(topicHandlers::delete));

        router.post("/projects/:project/topics/:topic/restore")
                .handler(wrapBlocking(topicHandlers::restore));

        setupFailureHandlers();
    }

    private void setupFailureHandlers() {
        router.getRoutes().forEach(this::setupFailureHandler);
    }

    @AfterEach
    void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    void createTopic_WithValidRequest_ShouldCreateTopicSuccessfully() throws InterruptedException {
        TopicResource topicResource = getTopicResource(project);
        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic();

        doReturn(varadhiTopic).when(varadhiTopicFactory).get(project, topicResource);

        TopicResource createdTopic = sendRequestWithEntity(
                createRequest(HttpMethod.POST, getTopicsUrl(project)),
                topicResource,
                TopicResource.class
        );

        assertEquals(topicResource.getProject(), createdTopic.getProject());
        verify(spanProvider).addSpan(REQUEST_SPAN_NAME);
        verify(varadhiTopicService).create(any(), eq(project));
    }

    @Test
    void createTopic_WithDuplicateResource_ShouldReturnConflict() throws InterruptedException {
        TopicResource topicResource = getTopicResource(project);

        doReturn(true).when(varadhiTopicService).exists(anyString());

        HttpResponse<Buffer> response = sendRequest(
                createRequest(HttpMethod.POST, getTopicsUrl(project)),
                JsonMapper.jsonSerialize(topicResource).getBytes()
        );

        assertEquals(409, response.statusCode());
        assertErrorResponse(response, "Topic 'project1.topic1' already exists.");
    }

    @Test
    void createTopic_WithMismatchedProjectName_ShouldReturnBadRequest() throws InterruptedException {
        TopicResource topicResource =
                getTopicResource(Project.of("differentProject", "", TEAM_NAME, ORG_NAME));

        HttpResponse<Buffer> response = sendRequest(
                createRequest(HttpMethod.POST, getTopicsUrl(project)),
                JsonMapper.jsonSerialize(topicResource).getBytes()
        );

        assertEquals(400, response.statusCode());
        assertErrorResponse(response, "Project name in URL and request body do not match.");
    }

    @Test
    void getTopic_WithValidRequest_ShouldReturnTopicSuccessfully() throws InterruptedException {
        TopicResource topicResource = getTopicResource(project);
        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic();
        String varadhiTopicName = String.join(".", project.getName(), TOPIC_NAME);

        doReturn(varadhiTopic).when(varadhiTopicService).get(varadhiTopicName);

        TopicResource retrievedTopic = sendRequestWithoutPayload(
                createRequest(HttpMethod.GET, getTopicUrl(project)),
                TopicResource.class
        );

        assertEquals(topicResource.getProject(), retrievedTopic.getProject());
    }

    @Test
    void listTopics_WithTopicsAvailable_ShouldReturnAllTopics() throws InterruptedException {
        List<String> topics = List.of(String.join(".", project.getName(), TOPIC_NAME));

        doReturn(topics).when(varadhiTopicService).getVaradhiTopics(project.getName());

        List<String> retrievedTopics = sendRequestWithoutPayload(
                createRequest(HttpMethod.GET, getTopicsUrl(project)),
                List.class
        );

        assertEquals(topics.size(), retrievedTopics.size());
    }

    @Test
    void listTopics_WithNoTopicsAvailable_ShouldReturnEmptyList() throws InterruptedException {
        doReturn(Collections.emptyList()).when(varadhiTopicService).getVaradhiTopics(project.getName());

        List<String> retrievedTopics = sendRequestWithoutPayload(
                createRequest(HttpMethod.GET, getTopicsUrl(project)),
                List.class
        );

        assertTrue(retrievedTopics.isEmpty());
    }

    @Test
    void deleteTopic_WithHardDelete_ShouldDeleteTopicSuccessfully() throws InterruptedException {
        verifyDeleteRequest("HARD_DELETE", ResourceDeletionType.HARD_DELETE);
    }

    @Test
    void deleteTopic_WithSoftDelete_ShouldDeleteTopicSuccessfully() throws InterruptedException {
        verifyDeleteRequest("SOFT_DELETE", ResourceDeletionType.SOFT_DELETE);
    }

    @Test
    void deleteTopic_WithNoDeletionType_ShouldDefaultToSoftDelete() throws InterruptedException {
        verifyDeleteRequest(null, ResourceDeletionType.SOFT_DELETE);
    }

    @Test
    void deleteTopic_WithInvalidDeletionType_ShouldDefaultToDefault() throws InterruptedException {
        verifyDeleteRequest("INVALID_TYPE", ResourceDeletionType.DEFAULT);
    }

    @Test
    void restoreTopic_WithValidRequest_ShouldRestoreTopicSuccessfully() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getTopicUrl(project) + "/restore");
        doNothing().when(varadhiTopicService).restore(any());

        sendRequestWithoutPayload(request, null);

        verify(varadhiTopicService).restore(any());
    }

    private void verifyDeleteRequest(String deletionType, ResourceDeletionType expectedDeletionType)
            throws InterruptedException {
        String url = getTopicUrl(project);
        if (deletionType != null) {
            url += "?deletionType=" + deletionType;
        }

        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, url);
        doNothing().when(varadhiTopicService).delete(any(), eq(expectedDeletionType));

        sendRequestWithoutPayload(request, null);

        verify(varadhiTopicService).delete(any(), eq(expectedDeletionType));
    }

    private void assertErrorResponse(HttpResponse<Buffer> response, String expectedReason) {
        ErrorResponse errorResponse = JsonMapper.jsonDeserialize(response.bodyAsString(), ErrorResponse.class);
        assertEquals(expectedReason, errorResponse.reason());
    }

    private TopicResource getTopicResource(Project project) {
        return TopicResource.grouped(TOPIC_NAME, project.getName(), Constants.DefaultTopicCapacity);
    }

    private String getTopicsUrl(Project project) {
        return String.join("/", "/projects", project.getName(), "topics");
    }

    private String getTopicUrl(Project project) {
        return String.join("/", getTopicsUrl(project), TOPIC_NAME);
    }
}
