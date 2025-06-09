package com.flipkart.varadhi.web.admin;

import java.util.Collections;
import java.util.List;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.utils.VaradhiTopicFactory;
import com.flipkart.varadhi.web.*;
import com.flipkart.varadhi.web.configurators.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.entities.TopicResource;
import com.flipkart.varadhi.web.routes.TelemetryType;
import com.flipkart.varadhi.web.v1.admin.TopicHandlers;
import io.opentelemetry.api.common.AttributeKey;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import lombok.experimental.ExtensionMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
class TopicHandlersTest extends WebTestBase {

    private static final String TOPIC_NAME = "topic1";
    private static final String TEAM_NAME = "team1";
    private static final String ORG_NAME = "org1";
    private static final String DEFAULT_PROJECT_NAME = "project1";

    private final Project project = Project.of(DEFAULT_PROJECT_NAME, "", TEAM_NAME, ORG_NAME);

    private TopicHandlers topicHandlers;

    @Mock
    private VaradhiTopicService varadhiTopicService;

    @Mock
    private VaradhiTopicFactory varadhiTopicFactory;

    private RequestTelemetryConfigurator requestTelemetryConfigurator;

    @BeforeEach
    void PreTest() throws InterruptedException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
        requestTelemetryConfigurator = RequestTelemetryConfigurator.getDefault(spanProvider);
        Resource.EntityResource<Project> projectResource = Resource.of(project, ResourceType.PROJECT);
        doReturn(projectResource).when(projectCache).getOrThrow(any());

        topicHandlers = new TopicHandlers(varadhiTopicFactory, varadhiTopicService, projectCache);

        setupRoutes();
    }

    private void setupRoutes() {
        router.post("/projects/:project/topics").handler(bodyHandler).handler(ctx -> {
            topicHandlers.setRequestBody(ctx);
            ctx.next();
        }).handler(ctx -> {
            requestTelemetryConfigurator.addRequestSpanAndLog(ctx, "CreateTopic", TelemetryType.ALL);
            ctx.next();
        }).handler(wrapBlocking(topicHandlers::create));

        router.get("/projects/:project/topics/:topic").handler(wrapBlocking(topicHandlers::get));

        router.get("/projects/:project/topics").handler(bodyHandler).handler(wrapBlocking(topicHandlers::list));

        router.delete("/projects/:project/topics/:topic").handler(wrapBlocking(topicHandlers::delete));

        router.patch("/projects/:project/topics/:topic/restore").handler(wrapBlocking(topicHandlers::restore));

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
            c(TopicResource.class)
        );

        assertEquals(topicResource.getProject(), createdTopic.getProject());
        var spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        assertEquals("server.request", spans.get(0).getName());
        assertEquals("CreateTopic", spans.get(0).getAttributes().get(AttributeKey.stringKey("api")));
        verify(varadhiTopicService).create(any(), eq(project));
    }

    @Test
    void createTopic_WithMismatchedProjectName_ShouldReturnBadRequest() throws InterruptedException {
        TopicResource topicResource = getTopicResource(Project.of("differentProject", "", TEAM_NAME, ORG_NAME));

        HttpResponse<Buffer> response = executeRequest(
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
            c(TopicResource.class)
        );

        assertEquals(topicResource.getProject(), retrievedTopic.getProject());
    }

    @Test
    void listTopics_WithTopicsAvailable_ShouldReturnAll() throws InterruptedException {
        List<String> topics = List.of(String.join(".", project.getName(), TOPIC_NAME));

        doReturn(topics).when(varadhiTopicService).getVaradhiTopics(project.getName(), false);

        List<String> retrievedTopics = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, getTopicsUrl(project)),
            c(List.class)
        );

        assertEquals(topics.size(), retrievedTopics.size());
    }

    @Test
    void listTopics_WithIncludeInactive_ShouldReturnAllIncludingInactive() throws InterruptedException {
        List<String> topics = List.of(String.join(".", project.getName(), TOPIC_NAME));

        doReturn(topics).when(varadhiTopicService).getVaradhiTopics(project.getName(), true);

        List<String> retrievedTopics = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, getTopicsUrl(project) + "?includeInactive=true"),
            c(List.class)
        );

        assertEquals(topics.size(), retrievedTopics.size());
    }

    @Test
    void listTopics_WithNoTopicsAvailable_ShouldReturnEmptyList() throws InterruptedException {
        doReturn(Collections.emptyList()).when(varadhiTopicService).getVaradhiTopics(project.getName(), false);

        List<String> retrievedTopics = sendRequestWithoutPayload(
            createRequest(HttpMethod.GET, getTopicsUrl(project)),
            c(List.class)
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
        verifyDeleteRequest("INVALID_TYPE", ResourceDeletionType.SOFT_DELETE);
    }

    @Test
    void restoreTopic_WithValidRequest_ShouldRestoreTopicSuccessfully() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PATCH, getTopicUrl(project) + "/restore");
        doNothing().when(varadhiTopicService).restore(any(), any());

        sendRequestWithoutPayload(request, null);

        verify(varadhiTopicService).restore(any(), any());
    }

    private void verifyDeleteRequest(String deletionType, ResourceDeletionType expectedDeletionType)
        throws InterruptedException {
        String url = getTopicUrl(project);
        if (deletionType != null) {
            url += "?deletionType=" + deletionType;
        }

        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, url);
        doNothing().when(varadhiTopicService).delete(any(), eq(expectedDeletionType), any());

        sendRequestWithoutPayload(request, null);

        verify(varadhiTopicService).delete(any(), eq(expectedDeletionType), any());
    }

    private void assertErrorResponse(HttpResponse<Buffer> response, String expectedReason) {
        ErrorResponse errorResponse = JsonMapper.jsonDeserialize(response.bodyAsString(), ErrorResponse.class);
        assertEquals(expectedReason, errorResponse.reason());
    }

    private TopicResource getTopicResource(Project project) {
        return TopicResource.grouped(
            TOPIC_NAME,
            project.getName(),
            Constants.DEFAULT_TOPIC_CAPACITY,
            LifecycleStatus.ActorCode.USER_ACTION,
            "test"
        );
    }

    private String getTopicsUrl(Project project) {
        return String.join("/", "/projects", project.getName(), "topics");
    }

    private String getTopicUrl(Project project) {
        return String.join("/", getTopicsUrl(project), TOPIC_NAME);
    }
}
