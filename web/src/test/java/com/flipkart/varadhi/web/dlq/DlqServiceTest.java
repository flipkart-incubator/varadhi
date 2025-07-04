package com.flipkart.varadhi.web.dlq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.flipkart.varadhi.common.TestHelper;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerApi;
import com.flipkart.varadhi.core.cluster.consumer.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.core.subscription.allocation.ShardAssignments;
import com.flipkart.varadhi.core.subscription.ShardDlqMessageResponse;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.web.DlqMessage;
import com.flipkart.varadhi.entities.web.DlqMessagesResponse;
import com.flipkart.varadhi.entities.web.DlqPageMarker;
import com.flipkart.varadhi.web.subscription.dlq.DlqService;
import com.flipkart.varadhi.web.v1.admin.SubscriptionTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.flipkart.varadhi.entities.Samples.PROJECT_1;
import static com.flipkart.varadhi.entities.Samples.U_TOPIC_RESOURCE_1;
import static com.flipkart.varadhi.entities.SubscriptionTestUtils.createUngroupedSubscription;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DlqServiceTest extends SubscriptionTestBase {

    private ControllerApi controllerClient;

    private ConsumerClientFactory consumerFactory;
    private ConsumerApi consumerClient;
    private DlqService dlqService;

    @BeforeEach
    public void setUp() {
        controllerClient = mock(ControllerApi.class);
        consumerFactory = mock(ConsumerClientFactory.class);
        consumerClient = mock(ConsumerApi.class);
        dlqService = new DlqService(controllerClient, consumerFactory);
    }

    @Test
    void testUnsideline() {
        VaradhiTopic vTopic = U_TOPIC_RESOURCE_1.toVaradhiTopic();
        VaradhiSubscription subscription = Mockito.spy(createUngroupedSubscription("sub12", PROJECT_1, vTopic));
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofFailedAt(System.currentTimeMillis());
        String requestedBy = "testUser";
        SubscriptionOperation operation = SubscriptionOperation.unsidelineOp(
            subscription.getName(),
            unsidelineRequest,
            requestedBy
        );

        when(subscription.isActive()).thenReturn(true);
        when(controllerClient.unsideline(anyString(), any(UnsidelineRequest.class), anyString())).thenReturn(
            CompletableFuture.completedFuture(operation)
        );

        CompletableFuture<SubscriptionOperation> result = dlqService.unsideline(
            subscription,
            unsidelineRequest,
            requestedBy
        );

        assertEquals(operation, result.join());
        verify(controllerClient).unsideline(subscription.getName(), unsidelineRequest, requestedBy);
    }

    @Test
    void testUnsidelineInvalidState() {
        VaradhiTopic vTopic = U_TOPIC_RESOURCE_1.toVaradhiTopic();
        VaradhiSubscription subscription = Mockito.spy(createUngroupedSubscription("sub12", PROJECT_1, vTopic));
        when(subscription.isActive()).thenReturn(false);
        InvalidOperationForResourceException exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> dlqService.unsideline(subscription, UnsidelineRequest.ofFailedAt(100), "testUser")
        );
        assertTrue(exception.getMessage().contains("Unsideline not allowed"));
    }

    @Test
    void testGetMessages() {
        long earliestFailedAt = System.currentTimeMillis();
        int limit = 10;

        VaradhiSubscription subscription = setupSubscriptionForGetMessages();
        List<DlqMessage> shard1Messages = List.of(createDlqMessage(1), createDlqMessage(2), createDlqMessage(1));
        String shard1NextPage = shard1Messages.get(1).getOffset().toString() + "," + shard1Messages.get(2)
                                                                                                   .getOffset()
                                                                                                   .toString();
        doReturn(CompletableFuture.completedFuture(new ShardDlqMessageResponse(shard1Messages, shard1NextPage))).when(
            consumerClient
        ).getMessagesByTimestamp(anyLong(), anyInt());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();

        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result = dlqService.getMessages(
            subscription,
            earliestFailedAt,
            pageMarkers,
            limit,
            recordWriter
        );
        result.join();
        assertEquals(subscription.getShards().getShardCount() + 1, msgRespones.size());
        Assertions.assertEquals(3, msgRespones.get(0).getMessages().size());
        Assertions.assertEquals(0, msgRespones.get(1).getMessages().size());
    }


    @Test
    void testGetMessagesNoMessages() {
        long earliestFailedAt = System.currentTimeMillis();
        int limit = 10;
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();

        doReturn(CompletableFuture.completedFuture(new ShardDlqMessageResponse(new ArrayList<>(), null))).when(
            consumerClient
        ).getMessagesByTimestamp(anyLong(), anyInt());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result = dlqService.getMessages(
            subscription,
            earliestFailedAt,
            pageMarkers,
            limit,
            recordWriter
        );
        TestHelper.assertValue(null, result);
        assertEquals(1, msgRespones.size());
        Assertions.assertTrue(msgRespones.getFirst().getMessages().isEmpty());
    }

    @Test
    void testGetMessagesShardGetMessageFails() {
        long earliestFailedAt = System.currentTimeMillis();
        int limit = 10;
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();
        doReturn(CompletableFuture.failedFuture(new IllegalArgumentException("Consumer not found for"))).when(
            consumerClient
        ).getMessagesByTimestamp(anyLong(), anyInt());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result = dlqService.getMessages(
            subscription,
            earliestFailedAt,
            pageMarkers,
            limit,
            recordWriter
        );
        TestHelper.assertException(result, IllegalArgumentException.class, "Consumer not found for");
        assertEquals(1, msgRespones.size());
        Assertions.assertTrue(msgRespones.getFirst().getError().contains("Consumer not found for"));
    }

    @Test
    void testGetMessagesInvalidState() {
        VaradhiTopic vTopic = U_TOPIC_RESOURCE_1.toVaradhiTopic();
        VaradhiSubscription subscription = Mockito.spy(createUngroupedSubscription("sub12", PROJECT_1, vTopic));
        when(subscription.isActive()).thenReturn(false);
        InvalidOperationForResourceException exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> dlqService.getMessages(
                subscription,
                System.currentTimeMillis(),
                DlqPageMarker.fromString(""),
                10,
                mock(Consumer.class)
            )
        );
        assertTrue(exception.getMessage().contains("Dlq messages can't be queried"));
    }

    @Test
    void testGetMessageGetSubAssignmentFails() {
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();
        doReturn(CompletableFuture.failedFuture(new IllegalArgumentException("Subscription not found"))).when(
            controllerClient
        ).getShardAssignments(anyString());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result = dlqService.getMessages(
            subscription,
            System.currentTimeMillis(),
            pageMarkers,
            10,
            recordWriter
        );
        TestHelper.assertException(result, IllegalArgumentException.class, "Subscription not found");
        assertEquals(1, msgRespones.size());
        Assertions.assertTrue(msgRespones.getFirst().getError().contains("Subscription not found"));
    }

    @Test
    void testGetMessageSubWithNoAssignments() {
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();
        doReturn(CompletableFuture.completedFuture(new ShardAssignments(new ArrayList<>()))).when(controllerClient)
                                                                                            .getShardAssignments(
                                                                                                anyString()
                                                                                            );
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result = dlqService.getMessages(
            subscription,
            System.currentTimeMillis(),
            pageMarkers,
            10,
            recordWriter
        );
        TestHelper.assertValue(null, result);
        assertEquals(1, msgRespones.size());
        Assertions.assertTrue(msgRespones.getFirst().getMessages().isEmpty());
        assertNull(msgRespones.getFirst().getError());
        assertNull(msgRespones.getFirst().getNextPage());
    }

    private VaradhiSubscription setupSubscriptionForGetMessages() {
        String consumerId = "consumerId";
        VaradhiTopic vTopic = U_TOPIC_RESOURCE_1.toVaradhiTopic();
        VaradhiSubscription subscription = Mockito.spy(createUngroupedSubscription("sub12", PROJECT_1, vTopic));
        SubscriptionShards shards = subscription.getShards();
        List<Assignment> assignments = new ArrayList<>();
        for (int i = 0; i < shards.getShardCount(); i++) {
            assignments.add(new Assignment(subscription.getName(), i, consumerId));
        }
        String subscriptionId = subscription.getName();
        when(subscription.isActive()).thenReturn(true);
        doReturn(CompletableFuture.completedFuture(new ShardAssignments(assignments))).when(controllerClient)
                                                                                      .getShardAssignments(
                                                                                          subscriptionId
                                                                                      );
        doReturn(consumerClient).when(consumerFactory).getInstance(consumerId);
        return subscription;
    }
}
