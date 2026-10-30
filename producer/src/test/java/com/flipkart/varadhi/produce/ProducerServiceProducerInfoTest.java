package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.entities.ProduceConfig;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.TopicProducerInfo;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicTestUtils;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProducerServiceProducerInfoTest {

    private static final String TOPIC = "project1.topic1";
    private static final String DEPLOYED = "region1";

    @SuppressWarnings ("unchecked")
    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topics = mock(ResourceReadCache.class);

    private ProducerService service;

    @BeforeEach
    void setUp() {
        service = new ProducerService(
            DEPLOYED,
            mock(ProducerFactory.class),
            mock(ResourceReadCache.class),
            mock(ResourceReadCache.class),
            topics
        );
    }

    @Test
    void reportsResolvedProducePath() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(
                RegionName.of(DEPLOYED),
                new ProduceConfig(TopicState.Producing, 0, RegionName.of("region2")),
                RegionName.of("region2"),
                ProduceConfig.blocked()
            )
        );
        when(topics.getOrThrow(TOPIC)).thenReturn(Resource.of(topic, ResourceType.TOPIC));

        TopicProducerInfo info = service.getProducerInfo(TOPIC);

        assertEquals(TOPIC, info.topicName());
        assertEquals(DEPLOYED, info.deployedRegion());
        assertEquals(TopicState.Producing, info.state());
        assertEquals("region2", info.produceRegion());
        assertEquals(0, info.storageTopicId());
        assertEquals("t", info.storageTopicName());
        assertTrue(info.produceAllowed());
        assertFalse(info.producerCached());
    }

    @Test
    void reportsTopicWithNoLocalProduceConfig() {
        VaradhiTopic topic = VaradhiTopicTestUtils.getNewTopic(
            Map.of(RegionName.of("region2"), ProduceConfig.producing())
        );
        when(topics.getOrThrow(TOPIC)).thenReturn(Resource.of(topic, ResourceType.TOPIC));

        TopicProducerInfo info = service.getProducerInfo(TOPIC);

        assertNull(info.state());
        assertNull(info.produceRegion());
        assertNull(info.storageTopicId());
        assertFalse(info.produceAllowed());
        assertFalse(info.producerCached());
    }

    @Test
    void missingTopicFails() {
        when(topics.getOrThrow(TOPIC)).thenThrow(new ResourceNotFoundException("missing"));
        assertThrows(ResourceNotFoundException.class, () -> service.getProducerInfo(TOPIC));
    }
}
