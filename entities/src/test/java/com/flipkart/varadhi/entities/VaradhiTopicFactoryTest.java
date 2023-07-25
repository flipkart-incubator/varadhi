package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class VaradhiTopicFactoryTest {
    private VaradhiTopicFactory varadhiTopicFactory;
    private StorageTopicFactory<StorageTopic> storageTopicFactory;
    private Project project;
    private String zone = "local";
    private String iTopicName;
    private String topicName = "testTopic";

    @BeforeEach
    public void setUp() {
        storageTopicFactory = mock(StorageTopicFactory.class);
        varadhiTopicFactory = new VaradhiTopicFactory(storageTopicFactory);
        project = new Project("default", "public", "public");
        String vTopicName = String.format("%s.%s", project.getName(), topicName);
        iTopicName = InternalTopic.internalMainTopicName(vTopicName, zone);
        String pTopicName =
                String.format("persistent://%s/%s/%s", project.getTenantName(), project.getName(), iTopicName);
        PulsarStorageTopic pTopic = new PulsarStorageTopic(pTopicName, 1);
        doReturn(pTopic).when(storageTopicFactory).getTopic(project, iTopicName, null);
    }

    @Test
    public void getTopic() {
        TopicResource topicResource = new TopicResource(
                topicName,
                1,
                project.getName(),
                true,
                false,
                null
        );
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        Assertions.assertNotNull(varadhiTopic);
        Assertions.assertEquals(1, varadhiTopic.getInternalTopics().size());
        InternalTopic it = varadhiTopic.getInternalTopics().get(iTopicName);
        StorageTopic st = it.getStorageTopic();
        Assertions.assertEquals(it.getStatus(), InternalTopic.ProduceStatus.Active);
        Assertions.assertEquals(it.getRegion(), zone);
        Assertions.assertNull(it.getSourceRegion());
        Assertions.assertNotNull(st);
        verify(storageTopicFactory, times(1)).getTopic(project, iTopicName, null);
    }
}
