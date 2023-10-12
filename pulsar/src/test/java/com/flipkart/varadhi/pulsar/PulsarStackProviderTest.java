package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.entities.CapacityHelper;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.MessagingStackOptions;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PulsarStackProviderTest {
    @TempDir
    Path tempDir;
    private PulsarStackProvider pulsarStackProvider;
    private MessagingStackOptions messagingStackOptions;
    private ObjectMapper objectMapper;
    private PulsarAdmin pulsarAdmin;
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
        pulsarAdmin = mock(PulsarAdmin.class);
        ClientProvider clientProvider = mock(ClientProvider.class);

        pulsarStackProvider = spy(new PulsarStackProvider());
        doReturn(clientProvider).when(pulsarStackProvider).getPulsarClientProvider(any(PulsarConfig.class));
        doReturn(pulsarAdmin).when(clientProvider).getAdminClient();
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
        PulsarStorageTopic topic = storageTopicFactory.getTopic(topicName, project, CapacityHelper.getDefault());
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
    public void testGetStorageTopicService_Initialized() throws PulsarAdminException {
        String topicName = "foobar";
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        StorageTopicService<PulsarStorageTopic> storageTopicService = pulsarStackProvider.getStorageTopicService();
        StorageTopicService<PulsarStorageTopic> storageTopicServiceSecond =
                pulsarStackProvider.getStorageTopicService();
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactory = pulsarStackProvider.getStorageTopicFactory();
        Assertions.assertEquals(storageTopicService, storageTopicServiceSecond);

        Topics topics = mock(Topics.class);
        doReturn(topics).when(pulsarAdmin).topics();
        doNothing().when(topics).createPartitionedTopic(anyString(), eq(1));
        PulsarStorageTopic pulsarStorageTopic =
                storageTopicFactory.getTopic(topicName, project, CapacityHelper.getDefault());
        storageTopicService.create(pulsarStorageTopic);
        verify(topics, times(1)).createPartitionedTopic(anyString(), eq(1));
    }
}

