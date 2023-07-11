package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class VaradhiTopicFactoryTest {
    private VaradhiTopicFactory varadhiTopicFactory;
    private StorageTopicFactory<StorageTopic> storageTopicFactory;

    @BeforeEach
    public void setUp() {
        storageTopicFactory = mock(StorageTopicFactory.class);
        varadhiTopicFactory = new VaradhiTopicFactory(storageTopicFactory);
        //TODO::check it, not a circular dependency though
        PulsarStorageTopic pTopic = new PulsarStorageTopic("public.default.testTopic.Main.local", 1);
        doReturn(pTopic).when(storageTopicFactory).getTopic("public.default.testTopic.Main.local", null);
    }

    @Test
    public void getTopic() {
        TopicResource topicResource = new TopicResource("testTopic", 1, "testProject", true, false, null);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(topicResource);
        Assertions.assertNotNull(varadhiTopic);
        Assertions.assertEquals(1, varadhiTopic.getInternalTopics().size());
        InternalTopic it = varadhiTopic.getInternalTopics().get(InternalTopic.TopicKind.Main);
        StorageTopic st = it.getStorageTopic();
        Assertions.assertEquals(it.getStatus(), InternalTopic.ProduceStatus.Active);
        Assertions.assertEquals(it.getRegion(), "local");
        Assertions.assertNull(it.getSourceRegion());
        Assertions.assertNotNull(st);
        verify(storageTopicFactory, times(1)).getTopic("public.default.testTopic.Main.local", null);
    }
}
