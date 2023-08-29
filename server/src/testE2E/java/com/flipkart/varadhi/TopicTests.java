package com.flipkart.varadhi;

import com.flipkart.varadhi.db.ZNode;
import com.flipkart.varadhi.entities.TopicResource;
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
    private static final String DefaultTenant = "public";
    private static final String DefaultProject = "default";


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

    @Test
    public void createTopic() {
        String topicName = "TestTopic24";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, false, null);
        TopicResource r = makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 200);
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, null);
        TopicResource r = makeCreateRequest(topic, getTopicCreateUri(DefaultTenant), 200);
        Assertions.assertEquals(topic.getVersion(), r.getVersion());
        Assertions.assertEquals(topic.getName(), r.getName());
        Assertions.assertEquals(topic.getProject(), r.getProject());
        Assertions.assertEquals(topic.isGrouped(), r.isGrouped());
        Assertions.assertNull(r.getCapacityPolicy());
        //TODO::fix this.
        String errorDuplicateTopic =
                String.format(
                        "Specified Topic(%s) already exists.",
                        ZNode.getResourceFQDN(topic.getProject(), topic.getName())
                );
        makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 500, errorDuplicateTopic, true);
        makeCreateRequest(topic, getTopicCreateUri(DefaultTenant), 409, errorDuplicateTopic, true);
    }

    @Test
    public void createTopicWithValidationFailure() {
        String topicName = "Test";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, null);
        String errorValidationTopic = "name: Varadhi Resource Name Length must be between 5 and 50";
        makeCreateRequest(topic, getTopicCreateUri(DefaultTenant), 500, errorValidationTopic, true);
    }

}
