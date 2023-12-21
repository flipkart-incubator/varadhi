package com.flipkart.varadhi.web.admin;

/**
 * @author kaur.prabhpreet
 * On 22/12/23
 */

import com.flipkart.varadhi.core.VaradhiTopicFactory;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.web.v1.admin.TopicHandlers;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.mockito.Mockito.*;

public class TopicHandlersTest {

    @Mock
    private VaradhiTopicFactory varadhiTopicFactory;
    @Mock
    private VaradhiTopicService varadhiTopicService;
    @Mock
    private MetaStore metaStore;
    @Mock
    private RoutingContext routingContext;
    @Mock
    private HttpServerRequest request;
    @Mock
    private HttpServerResponse response;
    @Mock
    private VaradhiTopic varadhiTopic;
    @Mock
    private Project project;
    @Mock
    private TopicResource topicResource;

    private TopicHandlers topicHandlers;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        topicHandlers = new TopicHandlers(varadhiTopicFactory, varadhiTopicService, metaStore);
        when(routingContext.request()).thenReturn(request);
        when(routingContext.response()).thenReturn(response);
    }

    @Test
    void getTopicSuccessfully() {
        when(request.getParam("project")).thenReturn("project1");
        when(request.getParam("topic")).thenReturn("topic1");
        when(metaStore.getVaradhiTopic("project1.topic1")).thenReturn(varadhiTopic);
        when(varadhiTopic.getTopicResource("project1")).thenReturn(topicResource);

        topicHandlers.get(routingContext);

        verify(response).end(anyString());
    }

    @Test
    void getTopicNotFound() {
        when(request.getParam("project")).thenReturn("project1");
        when(request.getParam("topic")).thenReturn("topic1");
        when(metaStore.getVaradhiTopic("project1.topic1")).thenReturn(null);

        topicHandlers.get(routingContext);

        verify(response).setStatusCode(404);
        verify(response).end(anyString());
    }

    @Test
    void createTopicSuccessfully() {
        when(request.getParam("project")).thenReturn("project1");
        when(routingContext.getBodyAsJson()).thenReturn(JsonObject.mapFrom(topicResource));
        when(topicResource.getProject()).thenReturn("project1");
        when(metaStore.getProject("project1")).thenReturn(project);
        when(metaStore.checkVaradhiTopicExists("project1.topic1")).thenReturn(false);

        topicHandlers.create(routingContext);

        verify(varadhiTopicService).create(any(), eq(project));
        verify(response).end(anyString());
    }

    @Test
    void createTopicAlreadyExists() {
        when(request.getParam("project")).thenReturn("project1");
        when(routingContext.getBodyAsJson()).thenReturn(JsonObject.mapFrom(topicResource));
        when(topicResource.getProject()).thenReturn("project1");
        when(metaStore.getProject("project1")).thenReturn(project);
        when(metaStore.checkVaradhiTopicExists("project1.topic1")).thenReturn(true);

        Assertions.assertThrows(DuplicateResourceException.class, () -> topicHandlers.create(routingContext));
    }

    @Test
    void deleteTopicSuccessfully() {
        when(request.getParam("project")).thenReturn("project1");
        when(request.getParam("topic")).thenReturn("topic1");
        when(metaStore.getVaradhiTopic("project1.topic1")).thenReturn(varadhiTopic);

        topicHandlers.delete(routingContext);

        verify(varadhiTopicService).delete(varadhiTopic);
        verify(response).end();
    }

    @Test
    void deleteTopicNotFound() {
        when(request.getParam("project")).thenReturn("project1");
        when(request.getParam("topic")).thenReturn("topic1");
        when(metaStore.getVaradhiTopic("project1.topic1")).thenReturn(null);

        topicHandlers.delete(routingContext);

        verify(response).setStatusCode(404);
        verify(response).end(anyString());
    }

    @Test
    void listTopicsSuccessfully() {
        when(request.getParam("project")).thenReturn("project1");
        when(metaStore.listVaradhiTopics("project1")).thenReturn(Arrays.asList("project1.topic1", "project1.topic2"));

        topicHandlers.listTopics(routingContext);

        verify(response).end(anyString());
    }
}
