package com.flipkart.varadhi.services;

import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.core.cluster.ControllerRestApi;
import com.flipkart.varadhi.core.cluster.entities.ShardAssignments;
import com.flipkart.varadhi.core.cluster.entities.ShardDlqMessageResponse;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.web.admin.SubscriptionTestBase;
import com.flipkart.varadhi.web.entities.DlqMessagesResponse;
import com.flipkart.varadhi.web.entities.DlqPageMarker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.flipkart.varadhi.TestHelper.assertException;
import static com.flipkart.varadhi.TestHelper.assertValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DlqServiceTest extends SubscriptionTestBase {

    private ControllerRestApi controllerClient;

    private ConsumerClientFactory consumerFactory;
    private ConsumerApi consumerClient;
    private DlqService dlqService;


    @BeforeEach
    public void setUp() {
        controllerClient = mock(ControllerRestApi.class);
        consumerFactory = mock(ConsumerClientFactory.class);
        consumerClient = mock(ConsumerApi.class);
        dlqService = new DlqService(controllerClient, consumerFactory);
        HeaderUtils.initialize(MessageHeaderUtils.fetchDummyHeaderConfiguration());
    }

    @Test
    void testUnsideline() {
        VaradhiTopic vTopic = topicResource.toVaradhiTopic();
        VaradhiSubscription subscription = spy(getUngroupedSubscription("sub12", project, vTopic));
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofFailedAt(System.currentTimeMillis());
        String requestedBy = "testUser";
        SubscriptionOperation operation =
                SubscriptionOperation.unsidelineOp(subscription.getName(), unsidelineRequest, requestedBy);

        when(subscription.isWellProvisioned()).thenReturn(true);
        when(controllerClient.unsideline(anyString(), any(UnsidelineRequest.class), anyString()))
                .thenReturn(CompletableFuture.completedFuture(operation));

        CompletableFuture<SubscriptionOperation> result =
                dlqService.unsideline(subscription, unsidelineRequest, requestedBy);

        assertEquals(operation, result.join());
        verify(controllerClient).unsideline(subscription.getName(), unsidelineRequest, requestedBy);
    }

    @Test
    void testUnsidelineInvalidState() {
        VaradhiTopic vTopic = topicResource.toVaradhiTopic();
        VaradhiSubscription subscription = spy(getUngroupedSubscription("sub12", project, vTopic));
        when(subscription.isWellProvisioned()).thenReturn(false);
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
        List<DlqMessage> shard1Messages = List.of(getDlqMessage(1), getDlqMessage(2), getDlqMessage(1));
        String shard1NextPage =
                shard1Messages.get(1).getOffset().toString() + "," + shard1Messages.get(2).getOffset().toString();
        doReturn(CompletableFuture.completedFuture(new ShardDlqMessageResponse(shard1Messages, shard1NextPage))).when(
                consumerClient).getMessagesByTimestamp(anyLong(), anyInt());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();

        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result =
                dlqService.getMessages(subscription, earliestFailedAt, pageMarkers, limit, recordWriter);
        result.join();
        assertEquals(subscription.getShards().getShardCount() + 1, msgRespones.size());
        assertEquals(3, msgRespones.get(0).getMessages().size());
        assertEquals(0, msgRespones.get(1).getMessages().size());
    }


    @Test
    void testGetMessagesNoMessages() {
        long earliestFailedAt = System.currentTimeMillis();
        int limit = 10;
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();

        doReturn(CompletableFuture.completedFuture(new ShardDlqMessageResponse(new ArrayList<>(), null))).when(
                consumerClient).getMessagesByTimestamp(anyLong(), anyInt());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result =
                dlqService.getMessages(subscription, earliestFailedAt, pageMarkers, limit, recordWriter);
        assertValue(null, result);
        assertEquals(1, msgRespones.size());
        assertTrue(msgRespones.getFirst().getMessages().isEmpty());
    }

    @Test
    void testGetMessagesShardGetMessageFails() {
        long earliestFailedAt = System.currentTimeMillis();
        int limit = 10;
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();
        doReturn(CompletableFuture.failedFuture(new IllegalArgumentException("Consumer not found for"))).when(consumerClient).getMessagesByTimestamp(anyLong(), anyInt());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result =
                dlqService.getMessages(subscription, earliestFailedAt, pageMarkers, limit, recordWriter);
        assertException(result, IllegalArgumentException.class, "Consumer not found for");
        assertEquals(1, msgRespones.size());
        assertTrue(msgRespones.getFirst().getError().contains("Consumer not found for"));
    }

    @Test
    void testGetMessagesInvalidState() {
        VaradhiTopic vTopic = topicResource.toVaradhiTopic();
        VaradhiSubscription subscription = spy(getUngroupedSubscription("sub12", project, vTopic));
        when(subscription.isWellProvisioned()).thenReturn(false);
        InvalidOperationForResourceException exception = assertThrows(
                InvalidOperationForResourceException.class,
                () -> dlqService.getMessages(
                        subscription, System.currentTimeMillis(), DlqPageMarker.fromString(""), 10,
                        mock(Consumer.class)
                )
        );
        assertTrue(exception.getMessage().contains("Dlq messages can't be queried"));
    }

    @Test
    void testGetMessageGetSubAssignmentFails() {
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();
        doReturn(CompletableFuture.failedFuture(new IllegalArgumentException("Subscription not found"))).when(controllerClient).getShardAssignments(anyString());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result =
                dlqService.getMessages(subscription, System.currentTimeMillis(), pageMarkers, 10, recordWriter);
        assertException(result, IllegalArgumentException.class, "Subscription not found");
        assertEquals(1, msgRespones.size());
        assertTrue(msgRespones.getFirst().getError().contains("Subscription not found"));
    }

    @Test
    void testGetMessageSubWithNoAssignments() {
        VaradhiSubscription subscription = setupSubscriptionForGetMessages();
        doReturn(CompletableFuture.completedFuture(new ShardAssignments(new ArrayList<>()))).when(controllerClient).getShardAssignments(anyString());
        List<DlqMessagesResponse> msgRespones = new ArrayList<>();
        Consumer<DlqMessagesResponse> recordWriter = msgRespones::add;
        DlqPageMarker pageMarkers = DlqPageMarker.fromString("");
        CompletableFuture<Void> result =
                dlqService.getMessages(subscription, System.currentTimeMillis(), pageMarkers, 10, recordWriter);
        assertValue(null, result);
        assertEquals(1, msgRespones.size());
        assertTrue(msgRespones.getFirst().getMessages().isEmpty());
        assertNull(msgRespones.getFirst().getError());
        assertNull(msgRespones.getFirst().getNextPage());
    }

    private VaradhiSubscription setupSubscriptionForGetMessages() {
        String consumerId = "consumerId";
        VaradhiTopic vTopic =  topicResource.toVaradhiTopic();
        VaradhiSubscription subscription = spy(getUngroupedSubscription("sub12", project, vTopic));
        SubscriptionShards shards = subscription.getShards();
        List<Assignment> assignments = new ArrayList<>();
        for (int i = 0; i < shards.getShardCount(); i++) {
            assignments.add(new Assignment(subscription.getName(), i, consumerId));
        }
        String subscriptionId = subscription.getName();
        when(subscription.isWellProvisioned()).thenReturn(true);
        doReturn(CompletableFuture.completedFuture(new ShardAssignments(assignments))).when(controllerClient)
                .getShardAssignments(subscriptionId);
        doReturn(consumerClient).when(consumerFactory).getInstance(consumerId);
        return subscription;
    }
}
