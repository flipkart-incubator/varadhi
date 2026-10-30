package com.flipkart.varadhi.web.v1.producer;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.TopicProducerInfo;
import com.flipkart.varadhi.entities.TopicProduceStats;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.TransitionNodeStatus;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;
import com.flipkart.varadhi.web.WebTestBase;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

public class TopicProducerInfoHandlerTest extends ProduceTestBase {

    private static final String ROUTE = "/projects/:project/topics/:topic/producer-info";
    private static final String PATH = "/projects/project1/topics/topic1/producer-info";

    @BeforeEach
    public void preTest() throws InterruptedException {
        super.setUp();
        Route route = router.get(ROUTE).handler(produceHandlers::producerInfo);
        setupFailureHandler(route);
    }

    @AfterEach
    public void postTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    public void returnsThisPodsProducerInfo() {
        TopicProducerInfo info = new TopicProducerInfo(
            topicFullName,
            deployedRegion,
            TopicState.Producing,
            deployedRegion,
            0,
            "storage",
            true,
            true,
            new TopicProduceStats(Map.of(1L, 2L), Map.of(1L, 1L)),
            TransitionType.TOPIC_FAILOVER,
            TransitionStage.PREPARE,
            TransitionNodeStatus.IN_PROGRESS
        );
        doReturn(info).when(producerService).getProducerInfo(topicFullName);

        HttpRequest<io.vertx.core.buffer.Buffer> request = createRequest(HttpMethod.GET, PATH);
        TopicProducerInfo got = sendRequestWithoutPayload(request, WebTestBase.c(TopicProducerInfo.class));

        assertEquals(topicFullName, got.topicName());
        assertEquals(deployedRegion, got.produceRegion());
        assertEquals(TopicState.Producing, got.state());
        assertTrue(got.producerCached());
        assertEquals(2L, got.produceStats().successCounts().get(1L));
        assertEquals(TransitionType.TOPIC_FAILOVER, got.transitionType());
        assertEquals(TransitionStage.PREPARE, got.transitionStage());
        assertEquals(TransitionNodeStatus.IN_PROGRESS, got.nodeStatus());
    }

    @Test
    public void missingTopicIs404() throws InterruptedException {
        doThrow(new ResourceNotFoundException("missing")).when(producerService).getProducerInfo(topicFullName);
        sendRequestWithoutPayload(createRequest(HttpMethod.GET, PATH), 404, "missing");
    }
}
