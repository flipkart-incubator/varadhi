package com.flipkart.varadhi.services;

import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.produce.otel.ProduceMetricProvider;
import com.flipkart.varadhi.produce.services.InternalTopicCache;
import com.flipkart.varadhi.produce.services.ProducerCache;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.flipkart.varadhi.utils.JsonMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static com.flipkart.varadhi.Constants.NAME_SEPARATOR;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_PRODUCE_IDENTITY;
import static com.flipkart.varadhi.MessageConstants.Headers.*;
import static com.flipkart.varadhi.MessageConstants.PRODUCE_CHANNEL_HTTP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ProducerServiceTests {
    ProducerService service;
    ProduceMetricProvider metricProvider;
    ProducerFactory producerFactory;
    VaradhiTopicService topicService;
    Producer producer;
    Random random;
    String topic = "topic1";
    String project = "project1";
    String region = "region1";

    @BeforeEach
    public void preTest() {
        producerFactory = mock(ProducerFactory.class);
        ProducerCache producerCache = new ProducerCache(producerFactory, "");

        topicService = mock(VaradhiTopicService.class);
        InternalTopicCache topicCache = new InternalTopicCache(topicService, "");

        metricProvider = new ProduceMetricProvider(new OtlpMeterRegistry());
        service = new ProducerService(producerCache, topicCache, metricProvider);
        random = new Random();

        producer = spy(new DummyProducer());
    }

    @Test
    public void testProduceMessage() throws InterruptedException {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());
        CompletableFuture<ProduceResult> result =
                service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project, topic), ctx);
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        verify(producer, times(1)).ProduceAsync(msg1);

        Message msg2 = getMessage(100, 1, null, 2000, ctx);
        result = service.produceToTopic(msg2, VaradhiTopic.buildTopicName(project, topic), ctx);
        rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertTrue(rc.produceResult.getProducerLatency() > 0);
        verify(producer, times(1)).ProduceAsync(msg2);
        verify(producerFactory, times(1)).getProducer(any());
    }

    @Test
    public void testProduceToNonExistingTopic() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(producer).when(producerFactory).getProducer(any());
        doThrow(new ResourceNotFoundException("Topic doesn't exists.")).when(topicService).get(vt.getName());
        //TODO:: This shall be ResourceNotFoundException, once ZKMetaStore code is fixed.
        Assertions.assertThrows(
                ProduceException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project, topic), ctx)
        );
        verify(producer, never()).ProduceAsync(any());
    }

    @Test
    public void testProduceWithUnknownExceptionInGetTopic() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(producer).when(producerFactory).getProducer(any());
        doThrow(new UncheckedExecutionException(new Exception())).when(topicService).get(vt.getName());
        Assertions.assertThrows(
                ProduceException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project, topic), ctx),
                "Produce failed due to internal error."
        );
        verify(producer, never()).ProduceAsync(any());
    }

    @Test
    public void produceToBlockedTopic() throws InterruptedException {
        produceNotAllowedTopicState(
                InternalTopic.TopicState.Blocked,
                ProduceResult.Status.Blocked,
                "Topic is blocked. Unblock the topic before produce."
        );
    }

    @Test
    public void produceToThrottledTopic() throws InterruptedException {
        produceNotAllowedTopicState(
                InternalTopic.TopicState.Throttled,
                ProduceResult.Status.Throttled,
                "Produce to Topic is currently rate limited, try again after sometime."
        );
    }

    @Test
    public void produceToReplicatingTopic() throws InterruptedException {
        produceNotAllowedTopicState(
                InternalTopic.TopicState.Replicating,
                ProduceResult.Status.NotAllowed,
                "Produce is not allowed for replicating topic."
        );
    }

    public void produceNotAllowedTopicState(
            InternalTopic.TopicState topicState, ProduceResult.Status status, String message
    ) throws InterruptedException {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topicState, topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());
        CompletableFuture<ProduceResult> result =
                service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project, topic), ctx);
        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(status, rc.produceResult.getProduceStatus().status());
        Assertions.assertEquals(message, rc.produceResult.getProduceStatus().message());
        verify(producer, never()).ProduceAsync(any());
    }

    @Test
    public void testProduceWithUnknownExceptionInGetProducer() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doThrow(new UncheckedExecutionException(new Exception())).when(producerFactory).getProducer(any());
        Assertions.assertThrows(
                ProduceException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project, topic), ctx),
                "Produce failed due to internal error."
        );
    }

    @Test
    public void testProduceWithknownExceptionInGetProducer() {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, null, 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doThrow(new ResourceNotFoundException("Topic doesn't exists.")).when(producerFactory).getProducer(any());
        //TODO:: This shall be ResourceNotFoundException, once ZKMetaStore code is fixed.
        Assertions.assertThrows(
                ProduceException.class,
                () -> service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project, topic), ctx)
        );
        verify(producer, never()).ProduceAsync(any());
    }

    @Test
    public void testProduceWithProducerFailure() throws InterruptedException {
        ProduceContext ctx = getProduceContext(topic, project, region);
        Message msg1 = getMessage(0, 1, RuntimeException.class.getName(), 0, ctx);
        VaradhiTopic vt = getTopic(topic, project, region);
        doReturn(vt).when(topicService).get(vt.getName());
        doReturn(producer).when(producerFactory).getProducer(any());

        CompletableFuture<ProduceResult> result =
                service.produceToTopic(msg1, VaradhiTopic.buildTopicName(project, topic), ctx);

        ResultCapture rc = getResult(result);
        Assertions.assertNotNull(rc.produceResult);
        Assertions.assertNull(rc.throwable);
        Assertions.assertEquals(ProduceResult.Status.Failed, rc.produceResult.getProduceStatus().status());
        Assertions.assertEquals(
                "Produce failed at messaging stack: java.lang.RuntimeException",
                rc.produceResult.getProduceStatus().message()
        );
        verify(producerFactory, times(1)).getProducer(any());
    }

    public VaradhiTopic getTopic(String name, String project, String region) {
        return getTopic(InternalTopic.TopicState.Producing, name, project, region);
    }

    public VaradhiTopic getTopic(InternalTopic.TopicState state, String name, String project, String region) {
        VaradhiTopic topic = VaradhiTopic.of(new TopicResource(name, 0, project, false, null));
        String itName = String.join(NAME_SEPARATOR, topic.getName(), region);
        StorageTopic st = new DummyStorageTopic(topic.getName(), 0);
        topic.addInternalTopic(new InternalTopic(itName, region, state, st));
        return topic;
    }

    public Message getMessage(int sleepMs, int offset, String exceptionClass, int payloadSize, ProduceContext ctx) {
        Multimap<String, String> headers = ArrayListMultimap.create();
        byte[] messageId = new byte[30];
        random.nextBytes(messageId);
        headers.put(MESSAGE_ID, new String(messageId));
        headers.put(PRODUCE_IDENTITY, ctx.getRequestContext().getProduceIdentity());
        headers.put(PRODUCE_REGION, ctx.getTopicContext().getRegion());
        headers.put(PRODUCE_TIMESTAMP, Long.toString(ctx.getRequestContext().getRequestTimestamp()));
        byte[] payload = null;
        if (payloadSize > 0) {
            payload = new byte[payloadSize];
            random.nextBytes(payload);
        }
        DummyProducer.DummyMessage message = new DummyProducer.DummyMessage(sleepMs, offset, exceptionClass, payload);
        return new Message(JsonMapper.jsonSerialize(message).getBytes(), headers);
    }

    public ProduceContext getProduceContext(String topic, String project, String region) {
        ProduceContext.RequestContext requestContext = new ProduceContext.RequestContext();
        requestContext.setRequestTimestamp(System.currentTimeMillis());
        requestContext.setBytesReceived(100);
        requestContext.setProduceIdentity(ANONYMOUS_PRODUCE_IDENTITY);
        requestContext.setRemoteHost("localhost");
        requestContext.setRequestChannel(PRODUCE_CHANNEL_HTTP);
        ProduceContext.TopicContext topicContext = new ProduceContext.TopicContext();
        topicContext.setTopicName(topic);
        topicContext.setProjectName(project);
        topicContext.setRegion(region);
        return new ProduceContext(requestContext, topicContext);
    }

    ResultCapture getResult(CompletableFuture<ProduceResult> future) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ResultCapture rc = new ResultCapture();
        future.whenComplete((pr, e) -> {
            rc.produceResult = pr;
            rc.throwable = e;
            latch.countDown();
        });
        latch.await();
        return rc;
    }

    static class ResultCapture {
        ProduceResult produceResult;
        Throwable throwable;
    }

    public static class DummyStorageTopic extends StorageTopic {
        public DummyStorageTopic(String name, int version) {
            super(name, version);
        }
    }

}
