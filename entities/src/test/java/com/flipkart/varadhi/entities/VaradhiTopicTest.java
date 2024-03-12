package com.flipkart.varadhi.entities;

/**
 * @author kaur.prabhpreet
 * On 22/12/23
 */

import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class VaradhiTopicTest {

    private static final String projectName = "project1";
    private static final String topicName = "topic1";

    @Test
    void buildTopicName() {
        String expected = String.join(".", projectName, topicName);
        String actual = VaradhiTopic.buildTopicName(projectName, topicName);
        assertEquals(expected, actual);
    }

    @Test
    void addAndGetInternalTopic() {
        VaradhiTopic varadhiTopic = VaradhiTopic.of(new TopicResource(topicName, 1, projectName, false, null));
        StorageTopic st = new DummyStorageTopic(varadhiTopic.getName(), 0);
        InternalCompositeTopic internalTopic = new InternalCompositeTopic("region1", TopicState.Producing, st);

        varadhiTopic.addInternalTopic(internalTopic);
        assertEquals(
                internalTopic.getTopicRegion(),
                varadhiTopic.getProduceTopicForRegion(internalTopic.getTopicRegion()).getTopicRegion()
        );
    }

    @Test
    void getTopicResource() {
        VaradhiTopic varadhiTopic = VaradhiTopic.of(
                new TopicResource(topicName, INITIAL_VERSION, projectName, false, CapacityPolicy.getDefault()));

        TopicResource topicResource = TopicResource.of(varadhiTopic);

        assertEquals(topicName, topicResource.getName());
        assertEquals(INITIAL_VERSION, topicResource.getVersion());
        assertEquals(projectName, topicResource.getProject());
        assertFalse(topicResource.isGrouped());
    }

    @EqualsAndHashCode(callSuper = true)
    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name, int version) {
            super(name, version);
        }
    }
}
