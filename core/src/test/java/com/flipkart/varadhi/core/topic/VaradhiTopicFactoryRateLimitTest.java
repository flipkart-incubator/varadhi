package com.flipkart.varadhi.core.topic;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.MessageSizeProfile;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RateLimiterMode;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.entities.web.TopicResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

class VaradhiTopicFactoryRateLimitTest {

    private static final String REGION = "local";
    private static final String TOPIC_NAME = "testTopic";

    @Mock
    private StorageTopicFactory<StorageTopic> storageTopicFactory;

    private Project project;
    private VaradhiTopicFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        project = Project.of("default", "", "public", "public");
        factory = new VaradhiTopicFactory(
            storageTopicFactory,
            REGION,
            Constants.DEFAULT_TOPIC_CAPACITY,
            RateLimiterMode.shadow,
            1024
        );

        String vTopicName = project.getName() + "." + TOPIC_NAME;
        String pTopicName = String.format("persistent://%s/%s", project.getOrg(), vTopicName);
        doReturn(PulsarStorageTopic.of(0, pTopicName, 1)).when(storageTopicFactory)
                                                         .getTopic(eq(0), eq(vTopicName), eq(project), any(), any());
    }

    @Test
    void get_DefaultsProduceRegionWeightsToEvenSplit() {
        TopicResource resource = baseResource(null, null, null);
        VaradhiTopic topic = factory.get(project, resource, VaradhiTopic.TopicCategory.TOPIC);

        assertEquals(Map.of(REGION, 1.0), topic.getProduceRegionWeights());
    }

    @Test
    void get_DefaultsMessageSizeProfileAndRateLimiterMode() {
        TopicResource resource = baseResource(null, null, null);
        VaradhiTopic topic = factory.get(project, resource, VaradhiTopic.TopicCategory.TOPIC);

        assertAll(
            () -> assertEquals(1024, topic.getMessageSizeProfile().getAvgMsgSizeBytes()),
            () -> assertEquals(1024, topic.getMessageSizeProfile().getMaxMsgSizeBytes()),
            () -> assertEquals(RateLimiterMode.shadow, topic.getRateLimiterMode())
        );
    }

    @Test
    void get_PreservesCapacityAsProvided() {
        TopicCapacityPolicy capacity = new TopicCapacityPolicy(0, 0, 2, 2);
        TopicResource resource = baseResource(capacity, null, null);
        VaradhiTopic topic = factory.get(project, resource, VaradhiTopic.TopicCategory.TOPIC);

        assertEquals(capacity, topic.getCapacity());
    }

    @Test
    void get_RejectsRegionWeightsThatDoNotSumToOne() {
        TopicResource resource = baseResource(null, Map.of(REGION, 0.5), null);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> factory.get(project, resource, VaradhiTopic.TopicCategory.TOPIC)
        );
        assertTrue(ex.getMessage().contains("sum to 1"));
    }

    @Test
    void get_RejectsInconsistentCapacity() {
        TopicResource resource = baseResource(new TopicCapacityPolicy(100, 1, 2, 2), null, null);
        resource.setMessageSizeProfile(new MessageSizeProfile(1024, 2048));

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> factory.get(project, resource, VaradhiTopic.TopicCategory.TOPIC)
        );
        assertTrue(ex.getMessage().contains("throughputKBps"));
    }

    private TopicResource baseResource(
        TopicCapacityPolicy capacity,
        Map<String, Double> regionWeights,
        RateLimiterMode mode
    ) {
        TopicResource resource = TopicResource.grouped(
            TOPIC_NAME,
            project.getName(),
            capacity,
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            "test"
        );
        resource.setProduceRegionWeights(regionWeights);
        resource.setRateLimiterMode(mode);
        return resource;
    }
}
