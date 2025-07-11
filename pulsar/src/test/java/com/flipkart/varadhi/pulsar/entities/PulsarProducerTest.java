package com.flipkart.varadhi.pulsar.entities;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.pulsar.PulsarTestBase;
import com.flipkart.varadhi.pulsar.config.ProducerOptions;
import com.flipkart.varadhi.pulsar.producer.PulsarProducer;
import com.google.common.collect.ArrayListMultimap;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static com.flipkart.varadhi.common.Constants.RANDOM_PARTITION_KEY_LENGTH;
import static org.mockito.Mockito.*;

public class PulsarProducerTest extends PulsarTestBase {

    PulsarClientImpl pulsarClient;
    ProducerBuilder<byte[]> producerBuilder;
    TypedMessageBuilderImpl<byte[]> messageBuilder;
    PartitionedProducerImpl<byte[]> producer;

    PulsarProducer pulsarProducer;
    ProducerOptions options;
    PulsarStorageTopic topic;

    TopicCapacityPolicy capacity;
    String hostname;

    @BeforeEach
    public void preTest() throws IOException, InterruptedException {
        pulsarClient = mock(PulsarClientImpl.class);

        producerBuilder = spy(new ProducerBuilderImpl<>(pulsarClient, Schema.BYTES));
        doReturn(producerBuilder).when(pulsarClient).newProducer();

        producer = mock(PartitionedProducerImpl.class);
        doReturn(producer).when(producerBuilder).create();

        messageBuilder = spy(new TypedMessageBuilderImpl(producer, Schema.BYTES));
        doReturn(messageBuilder).when(producer).newMessage();

        capacity = Constants.DEFAULT_TOPIC_CAPACITY;
        topic = PulsarStorageTopic.of(0, "one.two.three.four", 1);
        doReturn(topic.getName()).when(producer).getTopic();

        options = new ProducerOptions();
        hostname = "some_host_name";
    }


    @Test
    public void testProducerCreateWithDefaultOptions() throws PulsarClientException {
        ArgumentCaptor<Map<String, Object>> pConfigCaptor = ArgumentCaptor.forClass(Map.class);
        doReturn(producerBuilder).when(producerBuilder).loadConf(pConfigCaptor.capture());

        pulsarProducer = new PulsarProducer(pulsarClient, topic, capacity, options, hostname);
        Map<String, Object> pConfig = pConfigCaptor.getValue();
        validateProducerConfig(pConfig, topic, options, hostname);
    }

    @Test
    public void testProducerCreateWithCustomOptions() throws PulsarClientException {
        ArgumentCaptor<Map<String, Object>> pConfigCaptor = ArgumentCaptor.forClass(Map.class);
        doReturn(producerBuilder).when(producerBuilder).loadConf(pConfigCaptor.capture());

        options.setBatchingEnabled(false);
        options.setCompressionType(CompressionType.LZ4);
        options.setSendTimeoutMs(2000);
        options.setBatchingMaxPublishDelayMs(25);
        capacity = new TopicCapacityPolicy(1000, 2000, 1);
        topic = PulsarStorageTopic.of(0, "one.two.three.four", 1);
        doReturn(topic.getName()).when(producer).getTopic();

        pulsarProducer = new PulsarProducer(pulsarClient, topic, capacity, options, hostname);
        Map<String, Object> pConfig = pConfigCaptor.getValue();
        validateProducerConfig(pConfig, topic, options, hostname);
    }

    public void validateProducerConfig(
        Map<String, Object> pConfig,
        PulsarStorageTopic topic,
        ProducerOptions options,
        String hostname
    ) {
        Assertions.assertEquals(topic.getName(), pConfig.get("topicName"));
        Assertions.assertEquals(String.format("%s.%s", topic.getName(), hostname), pConfig.get("producerName"));
        Assertions.assertEquals(ProducerAccessMode.Shared, pConfig.get("accessMode"));

        Assertions.assertEquals(options.getSendTimeoutMs(), pConfig.get("sendTimeoutMs"));
        Assertions.assertEquals(options.isBlockIfQueueFull(), pConfig.get("blockIfQueueFull"));
        Assertions.assertEquals(options.isBatchingEnabled(), pConfig.get("batchingEnabled"));
        Assertions.assertEquals(options.getCompressionType(), pConfig.get("compressionType"));
        Assertions.assertEquals(
            options.getBatchingMaxPublishDelayMs() * 1000,
            pConfig.get("batchingMaxPublishDelayMicros")
        );

        Assertions.assertEquals(
            PulsarProducer.getMaxPendingMessages(capacity.getQps()),
            pConfig.get("maxPendingMessages")
        );
        Assertions.assertEquals(
            PulsarProducer.getMaxPendingMessages(capacity.getQps()),
            pConfig.get("maxPendingMessagesAcrossPartitions")
        );
        int batchMaxMessages = PulsarProducer.getBatchMaxMessages(
            capacity.getQps(),
            options.getBatchingMaxPublishDelayMs()
        );
        Assertions.assertEquals(batchMaxMessages, pConfig.get("batchingMaxMessages"));
        Assertions.assertEquals(
            PulsarProducer.getBatchingMaxBytes(batchMaxMessages, capacity),
            pConfig.get("batchingMaxBytes")
        );
    }

    Message getMessage(String payload) {
        ArrayListMultimap<String, String> requestHeaders = ArrayListMultimap.create();
        return new SimpleMessage(payload.getBytes(), requestHeaders);
    }

    @Test
    public void testMessageBuildOnSend() throws PulsarClientException {
        String payload = "somedata";
        pulsarProducer = new PulsarProducer(pulsarClient, topic, capacity, options, hostname);
        doReturn(CompletableFuture.completedFuture(new MessageIdImpl(1, 1, 1))).when(messageBuilder).sendAsync();
        Message message = getMessage(payload);
        pulsarProducer.produceAsync(message);
        org.apache.pulsar.client.api.Message<byte[]> actualMessage = messageBuilder.getMessage();
        Assertions.assertArrayEquals(payload.getBytes(), actualMessage.getData());
        Assertions.assertEquals(RANDOM_PARTITION_KEY_LENGTH, actualMessage.getKeyBytes().length);
        Assertions.assertEquals(producer.getProducerName(), actualMessage.getProducerName());
        Assertions.assertEquals(topic.getName(), actualMessage.getTopicName());
    }

    @Test
    public void testProducePartitioningKey() throws PulsarClientException {
        String payload = "somedata";
        String groupId1 = "groupId1";
        String groupId2 = "groupId2";
        pulsarProducer = new PulsarProducer(pulsarClient, topic, capacity, options, hostname);
        doReturn(CompletableFuture.completedFuture(new MessageIdImpl(1, 1, 1))).when(messageBuilder).sendAsync();
        Message message = getMessage(payload);
        message.getHeaders().put(StdHeaders.get().groupId(), groupId1);
        pulsarProducer.produceAsync(message);
        org.apache.pulsar.client.api.Message<byte[]> actualMessage = messageBuilder.getMessage();
        Assertions.assertArrayEquals(payload.getBytes(), actualMessage.getData());
        Assertions.assertEquals(groupId1, actualMessage.getKey());

        message.getHeaders().remove(StdHeaders.get().groupId(), groupId1);
        pulsarProducer.produceAsync(message);
        actualMessage = messageBuilder.getMessage();
        Assertions.assertArrayEquals(payload.getBytes(), actualMessage.getData());
        Assertions.assertNotEquals(groupId1, actualMessage.getKey());
        Assertions.assertEquals(RANDOM_PARTITION_KEY_LENGTH, actualMessage.getKeyBytes().length);

        message.getHeaders().put(StdHeaders.get().groupId(), groupId2);
        pulsarProducer.produceAsync(message);
        actualMessage = messageBuilder.getMessage();
        Assertions.assertArrayEquals(payload.getBytes(), actualMessage.getData());
        Assertions.assertEquals(groupId2, actualMessage.getKey());
    }

    @Test
    public void testMessageProperties() throws PulsarClientException {
        // all properties.
        // multi value properties
        String payload = "somedata";
        String groupId1 = "groupId1";
        pulsarProducer = new PulsarProducer(pulsarClient, topic, capacity, options, hostname);
        doReturn(CompletableFuture.completedFuture(new MessageIdImpl(1, 1, 1))).when(messageBuilder).sendAsync();
        Message message = getMessage(payload);
        message.getHeaders().put(StdHeaders.get().groupId(), groupId1);
        message.getHeaders().put("SomeHeader", "someheadervalue");
        message.getHeaders().put("x_foobar", "x_foobar_value");
        message.getHeaders().put("x_multivalue", "x_multivalue1");
        message.getHeaders().put("x_multivalue", "x_multivalue2");
        message.getHeaders().put("x_multivalue", "x_multivalue3");
        pulsarProducer.produceAsync(message);
        org.apache.pulsar.client.api.Message<byte[]> actualMessage = messageBuilder.getMessage();
        Map<String, String> properites = actualMessage.getProperties();
        Assertions.assertEquals("someheadervalue", properites.get("SomeHeader"));
        Assertions.assertEquals("x_foobar_value", properites.get("x_foobar"));
        Assertions.assertEquals(groupId1, properites.get(StdHeaders.get().groupId()));
        Assertions.assertEquals("x_multivalue1,x_multivalue2,x_multivalue3", properites.get("x_multivalue"));
    }

    @Test
    public void testSendAsyncThrows() throws PulsarClientException {
        String payload = "somedata";
        pulsarProducer = new PulsarProducer(pulsarClient, topic, capacity, options, hostname);
        doThrow(new RuntimeException("Some Internal Error.")).when(messageBuilder).sendAsync();
        Message message = getMessage(payload);
        RuntimeException ee = Assertions.assertThrows(
            RuntimeException.class,
            () -> pulsarProducer.produceAsync(message)
        );
        Assertions.assertEquals("Some Internal Error.", ee.getMessage());
    }

    @Test
    public void testSendAsyncFailsExceptionally() throws PulsarClientException {
        String payload = "somedata";
        pulsarProducer = new PulsarProducer(pulsarClient, topic, capacity, options, hostname);
        doReturn(CompletableFuture.failedFuture(new PulsarClientException.ProducerQueueIsFullError("Queue full.")))
                                                                                                                   .when(
                                                                                                                       messageBuilder
                                                                                                                   )
                                                                                                                   .sendAsync();
        Message message = getMessage(payload);
        CompletableFuture<Offset> future = pulsarProducer.produceAsync(message);
        Assertions.assertTrue(future.isCompletedExceptionally());
        ExecutionException ee = Assertions.assertThrows(
            ExecutionException.class,
            () -> future.get(1, TimeUnit.MILLISECONDS)
        );
        Assertions.assertTrue(ee.getCause() instanceof PulsarClientException.ProducerQueueIsFullError);
        Assertions.assertEquals("Queue full.", ee.getCause().getMessage());
    }
}
