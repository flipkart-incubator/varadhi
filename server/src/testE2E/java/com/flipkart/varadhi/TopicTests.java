package com.flipkart.varadhi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.db.ZKPathUtils;
import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.utils.JsonMapper;
import com.flipkart.varadhi.web.ErrorResponse;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

public class TopicTests {
    private static final int ConnectTimeoutMs = 10 * 1000;
    private static final int ReadTimeoutMs = 10 * 1000;

    private static final String VaradhiBaseUri = "http://localhost:8080";
    private static final String DefaultTenant = "DefaultTestTenant";
    private static final String DefaultProject = "TestProject";


    private Client getClient() {
        ClientConfig clientConfig = new ClientConfig().register(new ObjectMapperContextResolver());
        Client client = ClientBuilder.newClient(clientConfig);
        client.property(ClientProperties.CONNECT_TIMEOUT, ConnectTimeoutMs);
        client.property(ClientProperties.READ_TIMEOUT, ReadTimeoutMs);
        return client;
    }

    private String getTopicCreateUri(String tenant) {
        return String.format("%s/v1/tenants/%s/topics", VaradhiBaseUri, tenant);
    }

    private <T> T makeCreateRequest(T entity, String targetUrl, int expectedStatus) {
        Response response = getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE));
        Assertions.assertNotNull(response);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        Class<T> clazz = (Class<T>) entity.getClass();
        return response.readEntity(clazz);
    }

    private <T> void makeCreateRequest(
            T entity,
            String targetUrl,
            int expectedStatus,
            String expectedResponse,
            boolean isErrored
    ) {
        Response response = getClient()
                .target(targetUrl)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE));
        Assertions.assertNotNull(response);
        Assertions.assertEquals(expectedStatus, response.getStatus());
        if (null != expectedResponse) {
            String responseMsg =
                    isErrored ? response.readEntity(ErrorResponse.class).reason() : response.readEntity(String.class);
            Assertions.assertEquals(expectedResponse, responseMsg);
        }
    }

    @Test
    public void createTopic() {
        String topicName = "TestTopic24";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, false, null);
        TopicResource r = makeCreateRequest(topic, getTopicCreateUri(DefaultTenant), 200);
        Assertions.assertEquals(topic.getVersion(), r.getVersion());
        Assertions.assertEquals(topic.getName(), r.getName());
        Assertions.assertEquals(topic.getProject(), r.getProject());
        Assertions.assertEquals(topic.isGrouped(), r.isGrouped());
        Assertions.assertEquals(topic.isExclusiveSubscription(), r.isExclusiveSubscription());
        Assertions.assertNull(r.getCapacityPolicy());
        //TODO::fix this.
        String errorDuplicateTopic =
                String.format(
                        "Specified Topic(%s) already exists.",
                        ZKPathUtils.getTopicResourcePath(topic.getProject(), topic.getName())
                );
        makeCreateRequest(topic, getTopicCreateUri(DefaultTenant), 500, errorDuplicateTopic, true);
    }

    @Provider
    public class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

        private final ObjectMapper mapper = JsonMapper.getMapper();

        @Override
        public ObjectMapper getContext(Class<?> type) {
            return mapper;
        }
    }
}
