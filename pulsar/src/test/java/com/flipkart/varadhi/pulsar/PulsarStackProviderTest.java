package com.flipkart.varadhi.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.entities.StorageTopicFactory;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import com.flipkart.varadhi.pulsar.config.PulsarClientOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.services.MessagingStackOptions;
import com.flipkart.varadhi.services.StorageTopicService;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class PulsarStackProviderTest {
    private PulsarStackProvider pulsarStackProvider;
    private MessagingStackOptions messagingStackOptions;
    private ObjectMapper objectMapper;
    private PulsarAdmin pulsarAdmin;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        String yamlContent =
                "pulsarClientOptions:\n  pulsarUrl: \"http://127.0.0.1:8081\"\n  connectTimeout: 2000\n  readTimeout: 2000\n  requestTimeout: 2000\n";
        Path configFile = tempDir.resolve("pulsarConfig.yaml");
        Files.write(configFile, yamlContent.getBytes());

        messagingStackOptions = new MessagingStackOptions();
        messagingStackOptions.setConfigFile(configFile.toString());
        messagingStackOptions.setProviderClassName("com.flipkart.varadhi.pulsar.PulsarStackProvider");

        objectMapper = mock(ObjectMapper.class);
        pulsarAdmin = mock(PulsarAdmin.class);
        pulsarStackProvider = spy(new PulsarStackProvider());
        doReturn(pulsarAdmin).when(pulsarStackProvider).getPulsarAdminClient(any(PulsarClientOptions.class));
        doNothing().when(objectMapper).registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
    }

    @Test
    public void testInit() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        verify(objectMapper, times(1)).registerSubtypes(new NamedType(PulsarStorageTopic.class, "Pulsar"));//
    }

    @Test
    public void testGetStorageTopicFactory_NotInitialized() {
        assertThrows(InvalidStateException.class, () -> pulsarStackProvider.getStorageTopicFactory());
    }

    @Test
    public void testGetStorageTopicFactory_Initialized() {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactory = pulsarStackProvider.getStorageTopicFactory();
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactorySecond =
                pulsarStackProvider.getStorageTopicFactory();
        Assertions.assertEquals(storageTopicFactory, storageTopicFactorySecond);
        PulsarStorageTopic topic = storageTopicFactory.getTopic("foobar", null);
        Assertions.assertTrue(topic.getFqdn().endsWith("/foobar"));
        Assertions.assertEquals(1, topic.getPartitionCount());
    }

    @Test
    public void testGetStorageTopicService_NotInitialized() {
        assertThrows(InvalidStateException.class, () -> pulsarStackProvider.getStorageTopicService());
    }

    @Test
    public void testGetStorageTopicService_Initialized() throws PulsarAdminException {
        pulsarStackProvider.init(messagingStackOptions, objectMapper);
        StorageTopicService<PulsarStorageTopic> storageTopicService = pulsarStackProvider.getStorageTopicService();
        StorageTopicService<PulsarStorageTopic> storageTopicServiceSecond =
                pulsarStackProvider.getStorageTopicService();
        StorageTopicFactory<PulsarStorageTopic> storageTopicFactory = pulsarStackProvider.getStorageTopicFactory();
        Assertions.assertEquals(storageTopicService, storageTopicServiceSecond);

        Topics topics = mock(Topics.class);
        doReturn(topics).when(pulsarAdmin).topics();
        doNothing().when(topics).createPartitionedTopic(anyString(), anyInt(), any(Map.class));
        PulsarStorageTopic pulsarStorageTopic = storageTopicFactory.getTopic("foobar", null);
        storageTopicService.create(pulsarStorageTopic);
        verify(topics, times(1)).createPartitionedTopic(anyString(), anyInt(), any(Map.class));
    }
}

