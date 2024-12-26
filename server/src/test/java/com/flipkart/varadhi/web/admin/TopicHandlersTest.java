package com.flipkart.varadhi.web.admin;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.utils.VaradhiTopicFactory;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.web.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.entities.TopicResource;
import com.flipkart.varadhi.web.routes.TelemetryType;
import com.flipkart.varadhi.web.v1.admin.TopicHandlers;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.web.RequestTelemetryConfigurator.REQUEST_SPAN_NAME;
import static org.mockito.Mockito.*;

public class TopicHandlersTest extends WebTestBase {
    private final String topicName = "topic1";
    private final String team1 = "team1";
    private final String org1 = "org1";
    private final Project project = Project.of("project1", "", team1, org1);
    TopicHandlers topicHandlers;
    VaradhiTopicService varadhiTopicService;
    VaradhiTopicFactory varadhiTopicFactory;
    ProjectService projectService;
    RequestTelemetryConfigurator requestTelemetryConfigurator;
    SpanProvider spanProvider;
    Span span;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        spanProvider = mock(SpanProvider.class);
        span = mock(Span.class);
        doReturn(span).when(spanProvider).addSpan(REQUEST_SPAN_NAME);
        requestTelemetryConfigurator = new RequestTelemetryConfigurator(spanProvider, new SimpleMeterRegistry());

        varadhiTopicService = mock(VaradhiTopicService.class);
        varadhiTopicFactory = mock(VaradhiTopicFactory.class);
        projectService = mock(ProjectService.class);
        topicHandlers = new TopicHandlers(varadhiTopicFactory, varadhiTopicService, projectService);
        doReturn(project).when(projectService).getCachedProject(project.getName());

        Route routeCreate = router.post("/projects/:project/topics").handler(bodyHandler)
                .handler(bodyHandler).handler(ctx -> {
                    topicHandlers.setTopic(ctx);
                    ctx.next();
                })
                .handler(ctx -> {
                    requestTelemetryConfigurator.addRequestSpanAndLog(ctx, "CreateTopic", TelemetryType.ALL);
                    ctx.next();
                })
                .handler(wrapBlocking(topicHandlers::create));
        setupFailureHandler(routeCreate);
        Route routeGet = router.get("/projects/:project/topics/:topic").handler(wrapBlocking(topicHandlers::get));
        setupFailureHandler(routeGet);
        Route routeListAll = router.get("/projects/:project/topics").handler(bodyHandler)
                .handler(wrapBlocking(topicHandlers::listTopics));
        setupFailureHandler(routeListAll);
        Route routeDelete =
                router.delete("/projects/:project/topics/:topic").handler(wrapBlocking(topicHandlers::delete));
        setupFailureHandler(routeDelete);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void testTopicCreate() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getTopicsUrl(project));
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic vt = topicResource.toVaradhiTopic();
        doReturn(vt).when(varadhiTopicFactory).get(project, topicResource);
        TopicResource t1Created = sendRequestWithBody(request, topicResource, TopicResource.class);
        Assertions.assertEquals(topicResource.getProject(), t1Created.getProject());
        verify(spanProvider, times(1)).addSpan(eq(REQUEST_SPAN_NAME));
    }


    @Test
    public void testTopicGet() throws InterruptedException {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic t1 = topicResource.toVaradhiTopic();
        String varadhiTopicName = String.join(".", project.getName(), topicName);
        doReturn(t1).when(varadhiTopicService).get(varadhiTopicName);

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getTopicUrl(topicName, project));
        TopicResource t1Created = sendRequestWithoutBody(request, TopicResource.class);
        Assertions.assertEquals(topicResource.getProject(), t1Created.getProject());
    }

    @Test
    public void testListTopics() throws InterruptedException {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic t1 = topicResource.toVaradhiTopic();
        List<String> listOfTopics = new ArrayList<>();
        listOfTopics.add(t1.getName());

        doReturn(listOfTopics).when(varadhiTopicService).getVaradhiTopics(project.getName());

        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getTopicsUrl(project));
        List<String> t1Created = sendRequestWithoutBody(request, List.class);
        Assertions.assertEquals(t1Created.size(), listOfTopics.size());
    }

    @Test
    public void testTopicDelete() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, getTopicUrl(topicName, project));
        doNothing().when(varadhiTopicService).delete(any());

        sendRequestWithoutBody(request, null);
        verify(varadhiTopicService, times(1)).delete(any());
    }

    private TopicResource getTopicResource(String topicName, Project project) {
        return TopicResource.grouped(topicName,  project.getName(), Constants.DefaultTopicCapacity);
    }

    private String getTopicsUrl(Project project) {
        return String.join("/", "/projects", project.getName(), "topics");
    }


    private String getTopicUrl(String topicName, Project project) {
        return String.join("/", getTopicsUrl(project), topicName);
    }
}
