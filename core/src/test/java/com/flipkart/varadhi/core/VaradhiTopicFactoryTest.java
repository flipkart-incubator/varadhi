package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;
import static org.mockito.Mockito.*;

public class VaradhiTopicFactoryTest {
    private VaradhiTopicFactory varadhiTopicFactory;
    private StorageTopicFactory<StorageTopic> storageTopicFactory;
    private Project project;
    private String region = "local";
    private String vTopicName;
    private String topicName = "testTopic";

    @BeforeEach
    public void setUp() {
        storageTopicFactory = mock(StorageTopicFactory.class);
        varadhiTopicFactory = new VaradhiTopicFactory(storageTopicFactory, region);
        project = new Project("default", INITIAL_VERSION, "", "public", "public");
        vTopicName = String.format("%s.%s", project.getName(), topicName);
        String pTopicName =
                String.format("persistent://%s/%s", project.getOrg(), vTopicName);
        PulsarStorageTopic pTopic = new PulsarStorageTopic(pTopicName, 1);
        doReturn(pTopic).when(storageTopicFactory).getTopic(vTopicName, project, null);
    }

    @Test
    public void getTopic() {
        TopicResource topicResource = new TopicResource(
                topicName,
                1,
                project.getName(),
                true,
                null
        );
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        Assertions.assertNotNull(varadhiTopic);
        InternalTopic it = varadhiTopic.getProduceTopicForRegion(region);
        StorageTopic st = it.getStorageTopic();
        Assertions.assertEquals(it.getTopicState(), InternalTopic.TopicState.Producing);
        Assertions.assertEquals(it.getTopicRegion(), region);
        Assertions.assertNotNull(st);
        verify(storageTopicFactory, times(1)).getTopic(vTopicName, project, null);
    }
}
