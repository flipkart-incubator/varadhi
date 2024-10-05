package com.flipkart.varadhi.web.produce;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitterNoOpImpl;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.verticles.webserver.RateLimiterService;
import com.flipkart.varadhi.web.RequestTelemetryConfigurator;
import com.flipkart.varadhi.web.SpanProvider;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.produce.HeaderValidationHandler;
import com.flipkart.varadhi.web.v1.produce.ProduceHandlers;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.ext.web.Route;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ProduceTestBase extends WebTestBase {
    ProduceHandlers produceHandlers;
    ProducerService producerService;
    ProjectService projectService;
    String deployedRegion = "region1";
    String serviceHost = "localhost";
    RateLimiterService rateLimiterService;
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
        producerService = mock(ProducerService.class);
        spanProvider = mock(SpanProvider.class);
        RestOptions options = new RestOptions();
        options.setDeployedRegion(deployedRegion);
        requestTelemetryConfigurator = new RequestTelemetryConfigurator(spanProvider, new SimpleMeterRegistry());
        HeaderValidationHandler headerHandler = new HeaderValidationHandler(options);
        ProducerMetricHandler metricHandler = mock(ProducerMetricHandler.class);
        doReturn(new ProducerMetricsEmitterNoOpImpl()).when(metricHandler).getEmitter(anyInt(), any());
        rateLimiterService = mock(RateLimiterService.class);
        // no rate limit in tests
        doReturn(true).when(rateLimiterService).isAllowed(any(), anyLong());
        produceHandlers = new ProduceHandlers(deployedRegion, headerHandler::validate, producerService, projectService,
                metricHandler, rateLimiterService
        );
        route = router.post("/projects/:project/topics/:topic/produce");
        msgCapture = ArgumentCaptor.forClass(Message.class);
        messageId = "messageId1";
        payload = "somerandomdata".getBytes();
        Project project = Project.of("project1", "description", "team1", "org1");
        doReturn(project).when(projectService).getCachedProject("project1");
    }
}
