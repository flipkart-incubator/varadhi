package com.flipkart.varadhi;

import com.flipkart.varadhi.db.ZNode;
import com.flipkart.varadhi.entities.TopicResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicTests extends E2EBase {
    private static final String DefaultTenant = "DefaultTestTenant";
    private static final String DefaultProject = "TestProject";


    private String getTopicCreateUri(String tenant) {
        return String.format("%s/v1/tenants/%s/topics", VaradhiBaseUri, tenant);
    }

    @Test
    public void createTopic() {
        String topicName = "TestTopic24";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, DefaultProject, false, false, null);
        TopicResource r = makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 200);
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
                        ZNode.getResourceFQDN(topic.getProject(), topic.getName())
                );
        makeCreateRequest(getTopicCreateUri(DefaultTenant), topic, 500, errorDuplicateTopic, true);
    }

}
