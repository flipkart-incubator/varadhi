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
        CapacityPolicy capacityPolicy = CapacityPolicy.getDefault();
        PulsarStorageTopic pTopic = PulsarStorageTopic.from(pTopicName, capacityPolicy);
        doReturn(pTopic).when(storageTopicFactory).getTopic(vTopicName, project, capacityPolicy);
    }

    @Test
    public void getTopic() {
        CapacityPolicy capacityPolicy = CapacityPolicy.getDefault();
        TopicResource topicResource = new TopicResource(
                topicName,
                1,
                project.getName(),
                true,
                capacityPolicy
        );
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        Assertions.assertNotNull(varadhiTopic);
        InternalTopic it = varadhiTopic.getProduceTopicForRegion(region);
        StorageTopic st = it.getStorageTopic();
        Assertions.assertEquals(it.getTopicState(), TopicState.Producing);
        Assertions.assertEquals(it.getTopicRegion(), region);
        Assertions.assertNotNull(st);
        verify(storageTopicFactory, times(1)).getTopic(vTopicName, project, capacityPolicy);
    }

    @Test
    public void getTopicWithDefaultCapacity() {
        CapacityPolicy capacityPolicy = CapacityPolicy.getDefault();
        TopicResource topicResource = new TopicResource(
                topicName,
                1,
                project.getName(),
                true,
                null
        );
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        InternalTopic it = varadhiTopic.getProduceTopicForRegion(region);
        PulsarStorageTopic pt = (PulsarStorageTopic) it.getStorageTopic();
        Assertions.assertEquals(capacityPolicy.getMaxThroughputKBps(), pt.getMaxThroughputKBps());
        Assertions.assertEquals(capacityPolicy.getMaxQPS(), pt.getMaxQPS());
    }
}
