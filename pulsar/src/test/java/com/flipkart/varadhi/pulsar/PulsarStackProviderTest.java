package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.flipkart.varadhi.entities.VaradhiResource.INITIAL_VERSION;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PulsarStackProviderTest {
    @TempDir
    Path tempDir;
    private PulsarStackProvider pulsarStackProvider;
    private MessagingStackOptions messagingStackOptions;
    private ObjectMapper objectMapper;
    private Project project;

    @BeforeEach
    public void setUp() throws IOException {
        String yamlContent =
                "pulsarAdminOptions:\n  serviceHttpUrl: \"http://127.0.0.1:8081\"\npulsarClientOptions:\n  serviceUrl: \"http://127.0.0.1:8081\"\n";
        Path configFile = tempDir.resolve("pulsarConfig.yaml");
        Files.write(configFile, yamlContent.getBytes());

        messagingStackOptions = new MessagingStackOptions();
        messagingStackOptions.setConfigFile(configFile.toString());
        messagingStackOptions.setProviderClassName("com.flipkart.varadhi.pulsar.PulsarStackProvider");
        project = new Project("default", INITIAL_VERSION, "", "public", "public");
        objectMapper = mock(ObjectMapper.class);
        // Below is working as Pulsar clients doesn't seem to either create connections to actual hosts
        // or ignore failure (and retry later) during creation of client objects.
        pulsarStackProvider = spy(new PulsarStackProvider());
        doNothing().when(objectMapper).registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
    }

    @Test
    public void testInit() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
    }

    @Test
    public void testGetStorageTopicFactory_NotInitialized() {
        assertThrows(InvalidStateException.class, () -> pulsarStackProvider.getStorageTopicFactory());
    }

    @Test
    public void testGetStorageTopicFactory_Initialized() {
        String topicName = "foobar";
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactory = pulsarStackProvider.getStorageTopicFactory();
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactorySecond =
                pulsarStackProvider.getStorageTopicFactory();
        Assertions.assertEquals(storageTopicFactory, storageTopicFactorySecond);
        PulsarStorageTopic topic = storageTopicFactory.getTopic(topicName, project, CapacityPolicy.getDefault());
        Assertions.assertEquals(
                String.format("persistent://%s/%s/%s", project.getOrg(), project.getName(), topicName),
                topic.getName()
        );
        Assertions.assertEquals(1, topic.getPartitionCount());
    }

    @Test
    public void testGetStorageTopicService_NotInitialized() {
        assertThrows(InvalidStateException.class, () -> pulsarStackProvider.getStorageTopicService());
    }

    @Test
    public void testGetStorageTopicService_Initialized() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        StorageTopicService<PulsarStorageTopic> storageTopicService = pulsarStackProvider.getStorageTopicService();
        StorageTopicService<PulsarStorageTopic> storageTopicServiceSecond =
                pulsarStackProvider.getStorageTopicService();
        Assertions.assertEquals(storageTopicService, storageTopicServiceSecond);
    }

    @Test
    public void testGetProducerFactory_NotInitialized() {
        assertThrows(InvalidStateException.class, () -> pulsarStackProvider.getProducerFactory());
    }

    @Test
    public void testGetProducerFactory_Initialized() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        ProducerFactory<PulsarStorageTopic> producerFactory1 = pulsarStackProvider.getProducerFactory();
        ProducerFactory<PulsarStorageTopic> producerFactory2 = pulsarStackProvider.getProducerFactory();
        Assertions.assertEquals(producerFactory1, producerFactory2);
    }
}

