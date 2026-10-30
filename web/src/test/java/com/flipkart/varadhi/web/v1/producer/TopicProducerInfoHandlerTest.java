package com.flipkart.varadhi.web.v1.producer;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.TopicProducerInfo;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.web.WebTestBase;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            false
        );
        doReturn(info).when(producerService).getProducerInfo(topicFullName);

        HttpRequest<io.vertx.core.buffer.Buffer> request = createRequest(HttpMethod.GET, PATH);
        TopicProducerInfo got = sendRequestWithoutPayload(request, WebTestBase.c(TopicProducerInfo.class));

        assertEquals(topicFullName, got.topicName());
        assertEquals(deployedRegion, got.produceRegion());
        assertEquals(TopicState.Producing, got.state());
        assertFalse(got.producerCached());
    }

    @Test
    public void missingTopicIs404() throws InterruptedException {
        doThrow(new ResourceNotFoundException("missing")).when(producerService).getProducerInfo(topicFullName);
        sendRequestWithoutPayload(createRequest(HttpMethod.GET, PATH), 404, "missing");
    }
}
