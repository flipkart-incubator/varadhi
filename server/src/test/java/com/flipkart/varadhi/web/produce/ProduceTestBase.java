package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.core.entities.ApiContext;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProduceContext;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.web.ContextBuilder;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import io.vertx.ext.web.Route;
import org.apiguardian.api.API;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ProduceTestBase extends WebTestBase {
    ProduceHandlers produceHandlers;
    ProducerService producerService;
    ProjectService projectService;
    String deployedRegion = "region1";
    String serviceHost = "localhost";
    ContextBuilder contextBuilder;

    ArgumentCaptor<Message> msgCapture;
    ArgumentCaptor<ApiContext> ctxCapture;
    String topicPath = "/projects/project1/topics/topic1/produce";
    String topicFullName = "project1.topic1";
    String messageId;
    byte[] payload;

    Route route;

    @Override
    public void setUp() throws InterruptedException {
        super.setUp();
        projectService = mock(ProjectService.class);
        producerService = mock(ProducerService.class);
        RestOptions options = new RestOptions();
        options.setDeployedRegion(deployedRegion);
        contextBuilder = new ContextBuilder(projectService, deployedRegion, serviceHost );
        produceHandlers = new ProduceHandlers(options, producerService);
        route = router.post("/projects/:project/topics/:topic/produce");
        msgCapture = ArgumentCaptor.forClass(Message.class);
        ctxCapture = ArgumentCaptor.forClass(ApiContext.class);
        messageId = "messageId1";
        payload = "somerandomdata".getBytes();
        Project project = new Project("project1", 0, "description", "team1", "org1");
        doReturn(project).when(projectService).getCachedProject("project1");
    }
}
