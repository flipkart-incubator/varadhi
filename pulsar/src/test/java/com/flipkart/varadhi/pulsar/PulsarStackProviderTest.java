package com.flipkart.varadhi.pulsar;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.utils.YamlLoader;
import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.entities.PulsarSubscription;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PulsarStackProviderTest extends PulsarTestBase {
    private static final String PULSAR_CONFIG_TEST_FILE = "pulsar-config-test.yml";

    private PulsarStackProvider pulsarStackProvider;
    private MessagingStackOptions messagingStackOptions;
    private ObjectMapper objectMapper;
    private Project project;
    private TopicPlanner planner;

    @BeforeEach
    public void preTest() throws InterruptedException, IOException {
        PulsarConfig pulsarConfig = YamlLoader.loadConfig(PULSAR_CONFIG_TEST_FILE, PulsarConfig.class);

        messagingStackOptions = new MessagingStackOptions();
        messagingStackOptions.setConfigFile(PULSAR_CONFIG_TEST_FILE);
        messagingStackOptions.setProviderClassName("com.flipkart.varadhi.pulsar.PulsarStackProvider");
        project = Project.of("default", "", "public", "public");
        objectMapper = mock(ObjectMapper.class);
        // Below is working as Pulsar clients doesn't seem to either create connections to actual hosts
        // or ignore failure (and retry later) during creation of client objects.
        pulsarStackProvider = spy(new PulsarStackProvider());
        planner = new TopicPlanner(pulsarConfig);
        doNothing().when(objectMapper).registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
    }

    @Test
    public void testInit() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarStorageTopic.class, "PulsarTopic"));
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarSubscription.class, "PulsarSubscription"));
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarStorageTopic.class, "PulsarTopic"));
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarSubscription.class, "PulsarSubscription"));
    }

    @Test
    public void testGetStorageTopicFactory_NotInitialized() {
        assertThrows(IllegalStateException.class, () -> pulsarStackProvider.getStorageTopicFactory());
    }

    @Test
    public void testGetStorageTopicFactory_Initialized() {
        String topicName = "foobar";
        TopicCapacityPolicy capacity = Constants.DEFAULT_TOPIC_CAPACITY;
        InternalQueueCategory topicCategory = InternalQueueCategory.MAIN;
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactory = pulsarStackProvider.getStorageTopicFactory();
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactorySecond = pulsarStackProvider
                                                                                               .getStorageTopicFactory();
        Assertions.assertEquals(storageTopicFactory, storageTopicFactorySecond);
        PulsarStorageTopic topic = storageTopicFactory.getTopic(0, topicName, project, capacity, topicCategory);
        Assertions.assertEquals(
            String.format("persistent://%s/%s/%s", project.getOrg(), project.getName(), topicName),
            topic.getName()
        );
        Assertions.assertEquals(planner.getPartitionCount(capacity, topicCategory), topic.getPartitionCount());
    }

    @Test
    public void testGetStorageTopicService_NotInitialized() {
        assertThrows(IllegalStateException.class, () -> pulsarStackProvider.getStorageTopicService());
    }

    @Test
    public void testGetStorageTopicService_Initialized() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        var storageTopicService = pulsarStackProvider.getStorageTopicService();
        var storageTopicServiceSecond = pulsarStackProvider.getStorageTopicService();
        Assertions.assertEquals(storageTopicService, storageTopicServiceSecond);
    }

    @Test
    public void testGetProducerFactory_NotInitialized() {
        assertThrows(IllegalStateException.class, () -> pulsarStackProvider.getProducerFactory());
    }

    @Test
    public void testGetProducerFactory_Initialized() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        var producerFactory1 = pulsarStackProvider.getProducerFactory();
        var producerFactory2 = pulsarStackProvider.getProducerFactory();
        Assertions.assertEquals(producerFactory1, producerFactory2);
    }
}
