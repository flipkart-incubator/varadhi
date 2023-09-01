package com.flipkart.varadhi;

import com.flipkart.varadhi.db.ZNode;
import com.flipkart.varadhi.entities.TopicResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

<<<<<<< HEAD
public class TopicTests extends E2EBase {
    private static final String DefaultTenant = "DefaultTestTenant";
    private static final String DefaultProject = "TestProject";
=======
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

public class TopicTests extends E2EBase {
    private static final int ConnectTimeoutMs = 10 * 1000;
    private static final int ReadTimeoutMs = 10 * 1000;

    private static final String VaradhiBaseUri = "http://localhost:8080";
    private static final String DefaultTenant = "public";
    private static final String DefaultProject = "default";

>>>>>>> ee880fa69656aac241033658e2970aefe48a036f

    private String getTopicCreateUri(String tenant) {
        return String.format("%s/v1/tenants/%s/topics", VaradhiBaseUri, tenant);
    }

    @Test
    public void createTopic() {
        String topicName = "TestTopic24";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, null);
        TopicResource r = makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 200);
<<<<<<< HEAD
=======
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, null);
>>>>>>> ee880fa69656aac241033658e2970aefe48a036f
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
<<<<<<< HEAD
=======
        makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 500, errorDuplicateTopic, true);
>>>>>>> ee880fa69656aac241033658e2970aefe48a036f
        makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 409, errorDuplicateTopic, true);
    }

    @Test
    public void createTopicWithValidationFailure() {
        String topicName = "Test";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, null);
        String errorValidationTopic = "name: Varadhi Resource Name Length must be between 5 and 50";
        makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 500, errorValidationTopic, true);
    }

}
