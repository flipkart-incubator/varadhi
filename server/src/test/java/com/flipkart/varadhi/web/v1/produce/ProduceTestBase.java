package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.config.MessageHeaderUtils;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.EntityType;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitterNoOpImpl;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.OrgService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.web.configurators.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.WebTestBase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.ext.web.Route;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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

    RequestTelemetryConfigurator requestTelemetryConfigurator;
    SpanProvider spanProvider;

    Route route;

    @Override
    public void setUp() throws InterruptedException {
        super.setUp();
        projectService = mock(ProjectService.class);
        orgService = mock(OrgService.class);
        producerService = mock(ProducerService.class);
        spanProvider = mock(SpanProvider.class);
        varadhiTopicService = mock(VaradhiTopicService.class);
        RestOptions options = new RestOptions();
        options.setDeployedRegion(deployedRegion);
        requestTelemetryConfigurator = new RequestTelemetryConfigurator(spanProvider, new SimpleMeterRegistry());
        PreProduceHandler preProduceHandler = new PreProduceHandler();
        ProducerMetricHandler metricHandler = mock(ProducerMetricHandler.class);
        doReturn(new ProducerMetricsEmitterNoOpImpl()).when(metricHandler).getEmitter(anyInt(), any());
        VaradhiTopic topic = mock(VaradhiTopic.class);
        when(varadhiTopicService.get(any())).thenReturn(topic);
        when(topic.getNfrFilterName()).thenReturn(null);
        produceHandlers = new ProduceHandlers(
            producerService,
            preProduceHandler,
            metricHandler,
            MessageHeaderUtils.getTestConfiguration(),
            deployedRegion,
            projectCache
        );
        route = router.post("/projects/:project/topics/:topic/produce");
        msgCapture = ArgumentCaptor.forClass(Message.class);
        messageId = "messageId1";
        payload = "somerandomdata".getBytes();
        Project project = Project.of("project1", "description", "team1", "org1");
        Resource.EntityResource<Project> projectResource = Resource.of(project, EntityType.PROJECT);
        doReturn(projectResource).when(projectCache).getOrThrow(project.getName());
    }
}
