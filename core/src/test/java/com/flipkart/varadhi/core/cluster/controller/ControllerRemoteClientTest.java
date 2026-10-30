package com.flipkart.varadhi.core.cluster.controller;

import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.core.cluster.transition.TransitionBusAddress;
import com.flipkart.varadhi.core.cluster.transition.failover.TopicFailoverRequestWrapper;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.transition.TransitionAck;
import com.flipkart.varadhi.entities.cluster.transition.TransitionParticipation;
import com.flipkart.varadhi.entities.cluster.transition.TransitionStage;
import com.flipkart.varadhi.entities.cluster.transition.TransitionType;
import com.flipkart.varadhi.entities.cluster.transition.failover.ProduceFailoverTransition;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverOperation;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.flipkart.varadhi.core.cluster.controller.ControllerApi.ROUTE_CONTROLLER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ControllerRemoteClientTest {

    private static final String TOPIC = "proj.topic";
    private static final VaradhiTopicName TOPIC_FQN = VaradhiTopicName.parse(TOPIC);
    private static final String USER = "alice";
    private static final RegionName SOURCE = RegionName.of("r1");
    private static final RegionName TARGET = RegionName.of("r2");

    private MessageExchange exchange;
    private ControllerRemoteClient client;

    @BeforeEach
    void setUp() {
        exchange = mock(MessageExchange.class);
        client = new ControllerRemoteClient(exchange);
    }

    @Test
    void createTopicFailover_returnsOperationWhenExchangeSucceeds() {
        TopicFailoverOperation op = TopicFailoverOperation.of(TOPIC, USER);
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.CREATE_FAILOVER_API), any())).thenReturn(
            CompletableFuture.completedFuture(ResponseMessage.fromPayload(op, "req"))
        );

        TopicFailoverRequest body = new TopicFailoverRequest(SOURCE, TARGET, true, 5_000L, "ABORT");
        TopicFailoverOperation got = client.createTopicFailover(TOPIC_FQN, body, USER).join();

        assertEquals(op.getName(), got.getName());
        assertEquals(TOPIC, got.getTopicFqn());
        TopicFailoverRequestWrapper sent = captureRequest(TransitionBusAddress.CREATE_FAILOVER_API);
        assertEquals(TOPIC_FQN, sent.topicFqn());
        assertEquals(body, sent.request());
        assertEquals(USER, sent.requestedBy());
    }

    @Test
    void topicFailoverRequestWrapper_jsonRoundTripKeepsLagFields() {
        TopicFailoverRequest body = new TopicFailoverRequest(SOURCE, TARGET, true, 5_000L, "ABORT");
        TopicFailoverRequestWrapper original = new TopicFailoverRequestWrapper(TOPIC_FQN, body, USER);
        TopicFailoverRequestWrapper decoded = JsonMapper.jsonDeserialize(
            JsonMapper.jsonSerialize(original),
            TopicFailoverRequestWrapper.class
        );
        assertEquals(TOPIC_FQN, decoded.topicFqn());
        assertEquals(body, decoded.request());
        assertEquals(USER, decoded.requestedBy());
    }

    @Test
    void createTopicFailover_failsWhenExchangeFails() {
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.CREATE_FAILOVER_API), any())).thenReturn(
            CompletableFuture.failedFuture(new InvalidOperationForResourceException("already exists"))
        );

        CompletionException ex = assertThrows(
            CompletionException.class,
            () -> client.createTopicFailover(
                TOPIC_FQN,
                new TopicFailoverRequest(SOURCE, TARGET, false, null, null),
                USER
            ).join()
        );
        assertInstanceOf(InvalidOperationForResourceException.class, ex.getCause());
        assertEquals("already exists", ex.getCause().getMessage());
    }

    @Test
    void getTopicFailover_returnsTransitionWhenExchangeSucceeds() {
        ProduceFailoverTransition live = transition();
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.GET_FAILOVER_API), any())).thenReturn(
            CompletableFuture.completedFuture(ResponseMessage.fromPayload(live, "req"))
        );

        ProduceFailoverTransition got = client.getTopicFailover(TOPIC_FQN).join();

        assertEquals(live.getOperationId(), got.getOperationId());
        assertEquals(TOPIC, got.getTopicFqn());
        TopicFailoverRequestWrapper sent = captureRequest(TransitionBusAddress.GET_FAILOVER_API);
        assertEquals(TOPIC_FQN, sent.topicFqn());
        assertNull(sent.request());
        assertNull(sent.requestedBy());
    }

    @Test
    void getTopicFailover_failsWhenExchangeFails() {
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.GET_FAILOVER_API), any())).thenReturn(
            CompletableFuture.failedFuture(new ResourceNotFoundException("No active failover"))
        );

        CompletionException ex = assertThrows(
            CompletionException.class,
            () -> client.getTopicFailover(TOPIC_FQN).join()
        );
        assertInstanceOf(ResourceNotFoundException.class, ex.getCause());
        assertEquals("No active failover", ex.getCause().getMessage());
    }

    @Test
    void abortTopicFailover_returnsTransitionWhenExchangeSucceeds() {
        ProduceFailoverTransition live = transition();
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.ABORT_FAILOVER_API), any())).thenReturn(
            CompletableFuture.completedFuture(ResponseMessage.fromPayload(live, "req"))
        );

        ProduceFailoverTransition got = client.abortTopicFailover(TOPIC_FQN, USER).join();

        assertEquals(live.getOperationId(), got.getOperationId());
        TopicFailoverRequestWrapper sent = captureRequest(TransitionBusAddress.ABORT_FAILOVER_API);
        assertEquals(TOPIC_FQN, sent.topicFqn());
        assertEquals(USER, sent.requestedBy());
    }

    @Test
    void abortTopicFailover_failsWhenExchangeFails() {
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.ABORT_FAILOVER_API), any())).thenReturn(
            CompletableFuture.failedFuture(new ResourceNotFoundException("No active failover"))
        );

        CompletionException ex = assertThrows(
            CompletionException.class,
            () -> client.abortTopicFailover(TOPIC_FQN, USER).join()
        );
        assertInstanceOf(ResourceNotFoundException.class, ex.getCause());
    }

    @Test
    void getActiveFailovers_returnsListWhenExchangeSucceeds() {
        ProduceFailoverTransition live = transition();
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.LIST_FAILOVERS_API), any())).thenReturn(
            CompletableFuture.completedFuture(ResponseMessage.fromPayload(List.of(live), "req"))
        );

        List<ProduceFailoverTransition> got = client.getActiveFailovers().join();

        assertEquals(1, got.size());
        assertEquals(live.getOperationId(), got.getFirst().getOperationId());
        TopicFailoverRequestWrapper sent = captureRequest(TransitionBusAddress.LIST_FAILOVERS_API);
        assertNull(sent.topicFqn());
    }

    @Test
    void getActiveFailovers_returnsEmptyList() {
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.LIST_FAILOVERS_API), any())).thenReturn(
            CompletableFuture.completedFuture(ResponseMessage.fromPayload(List.of(), "req"))
        );

        assertTrue(client.getActiveFailovers().join().isEmpty());
    }

    @Test
    void getActiveFailovers_failsWhenExchangeFails() {
        when(exchange.request(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.LIST_FAILOVERS_API), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("bus down"))
        );

        CompletionException ex = assertThrows(CompletionException.class, () -> client.getActiveFailovers().join());
        assertEquals("bus down", ex.getCause().getMessage());
    }

    @Test
    void ack_completesWhenSendSucceeds() {
        TransitionAck ack = TransitionAck.success(
            "op-1",
            VaradhiTopicName.parse(TOPIC),
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.INVOLVED,
            "host-1",
            TransitionStage.PREPARE
        );
        when(exchange.send(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.TRANSITION_EVENT_ACK_API), any())).thenReturn(
            CompletableFuture.completedFuture(null)
        );

        client.ack(ack).join();

        ArgumentCaptor<ClusterMessage> captor = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(exchange).send(
            eq(ROUTE_CONTROLLER),
            eq(TransitionBusAddress.TRANSITION_EVENT_ACK_API),
            captor.capture()
        );
        assertEquals(ack, captor.getValue().getData(TransitionAck.class));
    }

    @Test
    void ack_failsWhenSendFails() {
        TransitionAck ack = TransitionAck.success(
            "op-1",
            VaradhiTopicName.parse(TOPIC),
            TransitionType.TOPIC_FAILOVER,
            TransitionParticipation.INVOLVED,
            "host-1",
            TransitionStage.PREPARE
        );
        when(exchange.send(eq(ROUTE_CONTROLLER), eq(TransitionBusAddress.TRANSITION_EVENT_ACK_API), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("send failed"))
        );

        CompletionException ex = assertThrows(CompletionException.class, () -> client.ack(ack).join());
        assertEquals("send failed", ex.getCause().getMessage());
    }

    private TopicFailoverRequestWrapper captureRequest(String api) {
        ArgumentCaptor<ClusterMessage> captor = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(exchange).request(eq(ROUTE_CONTROLLER), eq(api), captor.capture());
        return captor.getValue().getRequest(TopicFailoverRequestWrapper.class);
    }

    private static ProduceFailoverTransition transition() {
        return ProduceFailoverTransition.forFailover("op-1", TOPIC, SOURCE, TARGET, true);
    }
}
