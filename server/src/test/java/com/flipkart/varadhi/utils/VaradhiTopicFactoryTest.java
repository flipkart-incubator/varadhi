package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.web.entities.TopicResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class VaradhiTopicFactoryTest {
    private final String region = "local";
    private final String topicName = "testTopic";
    private VaradhiTopicFactory varadhiTopicFactory;
    private StorageTopicFactory<StorageTopic> storageTopicFactory;
    private Project project;
    private String vTopicName;

    @BeforeEach
    public void setUp() {
        storageTopicFactory = mock(StorageTopicFactory.class);
        varadhiTopicFactory = new VaradhiTopicFactory(storageTopicFactory, region, Constants.DefaultTopicCapacity);
        project = Project.of("default", "", "public", "public");
        vTopicName = String.format("%s.%s", project.getName(), topicName);
        String pTopicName =
                String.format("persistent://%s/%s", project.getOrg(), vTopicName);
        TopicCapacityPolicy capacityPolicy = Constants.DefaultTopicCapacity;
        PulsarStorageTopic pTopic = PulsarStorageTopic.of(pTopicName, 1, capacityPolicy);
        doReturn(pTopic).when(storageTopicFactory)
                .getTopic(vTopicName, project, capacityPolicy, InternalQueueCategory.MAIN);
    }

    @Test
    public void getTopic() {
        TopicCapacityPolicy capacityPolicy = Constants.DefaultTopicCapacity;
        TopicResource topicResource = TopicResource.grouped(topicName, project.getName(), capacityPolicy);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        Assertions.assertNotNull(varadhiTopic);
        InternalCompositeTopic it = varadhiTopic.getProduceTopicForRegion(region);
        StorageTopic st = it.getTopicToProduce();
        Assertions.assertEquals(it.getTopicState(), TopicState.Producing);
        Assertions.assertNotNull(st);
        verify(storageTopicFactory, times(1)).getTopic(vTopicName, project, capacityPolicy, InternalQueueCategory.MAIN);
    }

    @Test
    public void getTopicWithDefaultCapacity() {
        TopicCapacityPolicy capacityPolicy = Constants.DefaultTopicCapacity;
        TopicResource topicResource = TopicResource.grouped(topicName, project.getName(), null);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        InternalCompositeTopic it = varadhiTopic.getProduceTopicForRegion(region);
        PulsarStorageTopic pt = (PulsarStorageTopic) it.getTopicToProduce();
        Assertions.assertEquals(capacityPolicy.getThroughputKBps(), pt.getCapacity().getThroughputKBps());
        Assertions.assertEquals(capacityPolicy.getQps(), pt.getCapacity().getQps());
    }
}
