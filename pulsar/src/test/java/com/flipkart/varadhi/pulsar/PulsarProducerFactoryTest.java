package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.utils.YamlLoader;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.*;

public class PulsarProducerFactoryTest {
    @TempDir
    Path tempDir;
    ClientProvider clientProvider;
    PulsarStorageTopic topic;
    ProducerBuilder<byte[]> builder;

    @BeforeEach
    public void preTest() throws IOException {
        String yamlContent =
                "pulsarAdminOptions:\n  serviceHttpUrl: \"http://127.0.0.1:8081\"\npulsarClientOptions:\n  serviceUrl: \"http://127.0.0.1:8081\"\n";
        Path configFile = tempDir.resolve("pulsarConfig.yaml");
        Files.write(configFile, yamlContent.getBytes());
        PulsarConfig config = YamlLoader.loadConfig(configFile.toString(), PulsarConfig.class);
        topic = PulsarStorageTopic.from("testTopic", CapacityPolicy.getDefault());
        clientProvider = spy(new ClientProvider(config));
        PulsarClient pClient = mock(PulsarClient.class);
        builder = mock(ProducerBuilder.class);
        org.apache.pulsar.client.api.Producer producer = mock(org.apache.pulsar.client.api.Producer.class);
        doReturn(pClient).when(clientProvider).getPulsarClient();
        doReturn(builder).when(pClient).newProducer();
        doReturn(builder).when(builder).loadConf(any());
        doReturn(producer).when(builder).create();
    }

    @Test
    public void testGetProducer() throws PulsarClientException {
        PulsarProducerFactory factory = new PulsarProducerFactory(clientProvider, null, "localhost");
        Producer p = factory.getProducer(topic);
        Assertions.assertNotNull(p);
        verify(builder, times(1)).create();
    }

    @Test
    public void testGetProducerThrowsPulsarException() throws PulsarClientException {
        PulsarProducerFactory factory = new PulsarProducerFactory(clientProvider, null, "localhost");
        doThrow(new PulsarClientException.NotFoundException("Topic not found")).when(builder).create();
        ProduceException pe = Assertions.assertThrows(ProduceException.class, () -> factory.getProducer(topic));
        verify(builder, times(1)).create();
        Assertions.assertEquals(
                String.format("Failed to create Pulsar producer for %s. %s", topic.getName(), "Topic not found"),
                pe.getMessage()
        );
    }

    @Test
    public void testGetProducerThrowsUnhandledException() throws PulsarClientException {
        PulsarProducerFactory factory = new PulsarProducerFactory(clientProvider, null, "localhost");
        doThrow(new RuntimeException("Random error check")).when(builder).create();
        RuntimeException re = Assertions.assertThrows(RuntimeException.class, () -> factory.getProducer(topic));
        verify(builder, times(1)).create();
        Assertions.assertEquals("Random error check", re.getMessage());
    }
}
