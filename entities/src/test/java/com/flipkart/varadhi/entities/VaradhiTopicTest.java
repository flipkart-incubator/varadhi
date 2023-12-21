package com.flipkart.varadhi.entities;

/**
 * @author kaur.prabhpreet
 * On 22/12/23
 */

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class VaradhiTopicTest {

    @Test
    void buildTopicName() {
        String projectName = "project1";
        String topicName = "topic1";
        String expected = "project1.topic1";

        String actual = VaradhiTopic.buildTopicName(projectName, topicName);

        assertEquals(expected, actual);
    }

    @Test
    void addAndGetInternalTopic() {
        VaradhiTopic varadhiTopic = VaradhiTopic.of(new TopicResource("topic1", 1, "project1", false, null));
        StorageTopic st = new DummyStorageTopic(varadhiTopic.getName(), 0);
        InternalTopic internalTopic = new InternalTopic("region1", "topic1", TopicState.Producing, st);

        varadhiTopic.addInternalTopic(internalTopic);

        assertEquals(internalTopic, varadhiTopic.getProduceTopicForRegion("region1"));
    }

    @Test
    void getTopicResource() {
        VaradhiTopic varadhiTopic = VaradhiTopic.of(new TopicResource("topic1", 1, "project1", false, null));

        TopicResource topicResource = varadhiTopic.getTopicResource("project1");

        assertEquals("topic1", topicResource.getName());
        assertEquals(1, topicResource.getVersion());
        assertEquals("project1", topicResource.getProject());
        assertFalse(topicResource.isGrouped());
    }
    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name, int version) {
            super(name, version);
        }
    }
}
