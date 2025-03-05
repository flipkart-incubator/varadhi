package com.flipkart.varadhi.pulsar;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.producer.PulsarProducerFactory;
import com.flipkart.varadhi.spi.services.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.mockito.Mockito.*;

public class PulsarProducerFactoryTest extends PulsarTestBase {
    @TempDir
    Path tempDir;
    PulsarClient pClient;
    PulsarStorageTopic topic;
    ProducerBuilder<byte[]> builder;

    @BeforeEach
    public void preTest() throws IOException {
        String yamlContent =
            "pulsarAdminOptions:\n  serviceHttpUrl: \"http://127.0.0.1:8081\"\npulsarClientOptions:\n  serviceUrl: \"http://127.0.0.1:8081\"\n";
        Path configFile = tempDir.resolve("pulsarConfig.yaml");
        Files.write(configFile, yamlContent.getBytes());
        topic = PulsarStorageTopic.of("testTopic", 1, Constants.DEFAULT_TOPIC_CAPACITY);
        pClient = mock(PulsarClient.class);
        builder = mock(ProducerBuilder.class);
        org.apache.pulsar.client.api.Producer producer = mock(org.apache.pulsar.client.api.Producer.class);
        doReturn(builder).when(pClient).newProducer();
        doReturn(builder).when(builder).loadConf(any());
        doReturn(producer).when(builder).create();
    }

    @Test
    public void testGetProducer() throws PulsarClientException {
        PulsarProducerFactory factory = new PulsarProducerFactory(pClient, null, "localhost");
        Producer p = factory.newProducer(topic);
        Assertions.assertNotNull(p);
        verify(builder, times(1)).create();
    }

    @Test
    public void testGetProducerThrowsPulsarException() throws PulsarClientException {
        PulsarProducerFactory factory = new PulsarProducerFactory(pClient, null, "localhost");
        doThrow(new PulsarClientException.NotFoundException("Topic not found")).when(builder).create();
        ProduceException pe = Assertions.assertThrows(ProduceException.class, () -> factory.newProducer(topic));
        verify(builder, times(1)).create();
        Assertions.assertEquals(
            String.format("Failed to create Pulsar producer for %s. %s", topic.getName(), "Topic not found"),
            pe.getMessage()
        );
    }

    @Test
    public void testGetProducerThrowsUnhandledException() throws PulsarClientException {
        PulsarProducerFactory factory = new PulsarProducerFactory(pClient, null, "localhost");
        doThrow(new RuntimeException("Random error check")).when(builder).create();
        RuntimeException re = Assertions.assertThrows(RuntimeException.class, () -> factory.newProducer(topic));
        verify(builder, times(1)).create();
        Assertions.assertEquals("Random error check", re.getMessage());
    }
}
