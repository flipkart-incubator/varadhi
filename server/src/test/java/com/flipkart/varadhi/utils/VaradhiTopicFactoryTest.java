package com.flipkart.varadhi.utils;

import java.lang.reflect.Method;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.core.topic.VaradhiTopicFactory;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.entities.web.TopicResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class VaradhiTopicFactoryTest {

    private static final String REGION = "local";
    private static final String TOPIC_NAME = "testTopic";
    private static final TopicCapacityPolicy CAPACITY_POLICY = Constants.DEFAULT_TOPIC_CAPACITY;

    @Mock
    private StorageTopicFactory<StorageTopic> storageTopicFactory;

    @InjectMocks
    private VaradhiTopicFactory varadhiTopicFactory;

    private Project project;
    private String vTopicName;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        varadhiTopicFactory = new VaradhiTopicFactory(storageTopicFactory, REGION, Constants.DEFAULT_TOPIC_CAPACITY);

        project = Project.of("default", "", "public", "public");
        vTopicName = String.format("%s.%s", project.getName(), TOPIC_NAME);
        String pTopicName = String.format("persistent://%s/%s", project.getOrg(), vTopicName);
        PulsarStorageTopic pTopic = PulsarStorageTopic.of(0, pTopicName, 1);

        doReturn(pTopic).when(storageTopicFactory)
                        .getTopic(vTopicName, project, CAPACITY_POLICY, InternalQueueCategory.MAIN);
    }

    @Test
    void get_WithValidTopicResource_ShouldReturnValidVaradhiTopic() {
        TopicResource topicResource = TopicResource.grouped(
            TOPIC_NAME,
            project.getName(),
            CAPACITY_POLICY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);

        assertNotNull(varadhiTopic);
        SegmentedStorageTopic internalTopic = varadhiTopic.getProduceTopicForRegion(REGION);
        assertEquals(TopicState.Producing, internalTopic.getTopicState());
        assertNotNull(internalTopic.getTopicToProduce());

        verify(storageTopicFactory, times(1)).getTopic(
            vTopicName,
            project,
            CAPACITY_POLICY,
            InternalQueueCategory.MAIN
        );
    }

    @Test
    void get_WhenNoCapacityPolicyProvided_ShouldUseDefaultCapacity() {
        TopicResource topicResource = TopicResource.grouped(
            TOPIC_NAME,
            project.getName(),
            null,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        SegmentedStorageTopic internalTopic = varadhiTopic.getProduceTopicForRegion(REGION);
        PulsarStorageTopic storageTopic = (PulsarStorageTopic)internalTopic.getTopicToProduce();

        assertNotNull(storageTopic);
        assertEquals(CAPACITY_POLICY, varadhiTopic.getCapacity());
    }

    @Test
    void planDeployment_ValidVaradhiTopic_ShouldInvokeStorageTopicCreation() throws Exception {
        TopicResource topicResource = TopicResource.grouped(
            TOPIC_NAME,
            project.getName(),
            Constants.DEFAULT_TOPIC_CAPACITY,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        VaradhiTopic varadhiTopic = topicResource.toVaradhiTopic();

        Method planDeploymentMethod = VaradhiTopicFactory.class.getDeclaredMethod(
            "planDeployment",
            Project.class,
            VaradhiTopic.class
        );
        planDeploymentMethod.setAccessible(true);

        planDeploymentMethod.invoke(varadhiTopicFactory, project, varadhiTopic);

        SegmentedStorageTopic internalCompositeTopic = varadhiTopic.getProduceTopicForRegion(REGION);
        assertNotNull(internalCompositeTopic);
        assertEquals(TopicState.Producing, internalCompositeTopic.getTopicState());
        assertNotNull(internalCompositeTopic.getTopicToProduce());

        verify(storageTopicFactory, times(1)).getTopic(
            vTopicName,
            project,
            Constants.DEFAULT_TOPIC_CAPACITY,
            InternalQueueCategory.MAIN
        );
    }
}
