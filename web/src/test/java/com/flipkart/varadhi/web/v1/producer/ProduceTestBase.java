package com.flipkart.varadhi.web.v1.producer;

import com.flipkart.varadhi.core.config.MessageHeaderUtils;
import com.flipkart.varadhi.core.config.MetricsOptions;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.produce.ProducerService;
import com.flipkart.varadhi.core.OrgService;
import com.flipkart.varadhi.core.ProjectService;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.web.configurators.MsgProduceRequestTelemetryConfigurator;
import com.flipkart.varadhi.web.WebTestBase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.ext.web.Route;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProduceTestBase extends WebTestBase {
    ProduceHandlers produceHandlers;
    ProducerService producerService;
    ProjectService projectService;
    OrgService orgService;
    VaradhiTopicService varadhiTopicService;
    String deployedRegion = "region1";

    ArgumentCaptor<Message> msgCapture;
    String topicPath = "/projects/project1/topics/topic1/produce";
    String topicFullName = "project1.topic1";
    String messageId;
    byte[] payload;

    MsgProduceRequestTelemetryConfigurator telemetryConfigurator;
    Route route;

    @Override
    public void setUp() throws InterruptedException {
        super.setUp();
        projectService = mock(ProjectService.class);
        orgService = mock(OrgService.class);
        producerService = mock(ProducerService.class);
        varadhiTopicService = mock(VaradhiTopicService.class);
        telemetryConfigurator = new MsgProduceRequestTelemetryConfigurator(
            spanProvider,
            new SimpleMeterRegistry(),
            MetricsOptions.getDefault()
        );
        VaradhiTopic topic = mock(VaradhiTopic.class);
        when(varadhiTopicService.get(any())).thenReturn(topic);
        when(topic.getNfrFilterName()).thenReturn(null);
        produceHandlers = new ProduceHandlers(
            producerService,
            MessageHeaderUtils.getTestConfiguration(),
            deployedRegion,
            projectCache
        );
        route = router.post("/projects/:project/topics/:topic/produce");
        msgCapture = ArgumentCaptor.forClass(Message.class);
        messageId = "messageId1";
        payload = "somerandomdata".getBytes();
        Project project = Project.of("project1", "description", "team1", "org1");
        Resource.EntityResource<Project> projectResource = Resource.of(project, ResourceType.PROJECT);
        doReturn(projectResource).when(projectCache).getOrThrow(project.getName());
    }
}
