package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.core.cluster.ConsumerApi;
import com.flipkart.varadhi.core.cluster.ConsumerClientFactory;
import com.flipkart.varadhi.entities.NodeProvider;
import com.flipkart.varadhi.entities.SubProvider;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.*;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.OpStore;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.flipkart.varadhi.TestHelper.*;
import static com.flipkart.varadhi.entities.NodeProvider.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ControllerApiMgrTest {

    ControllerApiMgr controllerApiMgr;
    ConsumerApi consumerApi;
    AssignmentManager assignmentManager;
    OperationMgr operationMgr;
    MetaStore metaStore;
    OpStore opStore;
    ConsumerClientFactory consumerClientFactory;
    String requestedBy = "testRequester";

    @BeforeEach
    public void preTest() {
        ControllerConfig config = mock(ControllerConfig.class);
        doReturn(2).when(config).getMaxConcurrentOps();
        consumerClientFactory = mock(ConsumerClientFactory.class);
        metaStore = mock(MetaStore.class);
        consumerApi = mock(ConsumerApi.class);
        assignmentManager = mock(AssignmentManager.class);
        opStore = mock(OpStore.class);
        operationMgr = spy(new OperationMgr(config.getMaxConcurrentOps(), opStore, new RetryPolicy(0, 4, 5, 20)));
        when(consumerClientFactory.getInstance(anyString())).thenReturn(consumerApi);
        controllerApiMgr = spy(new ControllerApiMgr(operationMgr, assignmentManager, metaStore, consumerClientFactory));
    }

    @Test
    public void testAddConsumerNode() {
        MemberInfo memberInfo =
                new MemberInfo("Consumer.01", 0, new ComponentKind[]{ComponentKind.Consumer}, new NodeCapacity());
        ConsumerInfo consumerInfo = ConsumerInfo.from(memberInfo);
        ConsumerNode consumerNode = new ConsumerNode(memberInfo);
        doReturn(CompletableFuture.completedFuture(consumerInfo)).when(consumerApi).getConsumerInfo();

        CompletableFuture<String> result = controllerApiMgr.addConsumerNode(consumerNode);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        verify(assignmentManager, times(1)).addConsumerNode(consumerNode);
        assertValue(consumerNode.getConsumerId(), result);
    }

    @Test
    public void testAddConsumerNodeWhenGetConsumerInfoFailsExceptionally() {
        MemberInfo memberInfo =
                new MemberInfo("Consumer.01", 0, new ComponentKind[]{ComponentKind.Consumer}, new NodeCapacity());
        ConsumerNode consumerNode = new ConsumerNode(memberInfo);
        doReturn(CompletableFuture.failedFuture(
                new ReplyException(ReplyFailure.NO_HANDLERS, "Host not available."))).when(consumerApi)
                .getConsumerInfo();

        CompletableFuture<String> result = controllerApiMgr.addConsumerNode(consumerNode);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        verify(assignmentManager, times(0)).addConsumerNode(consumerNode);
        assertException(result, ReplyException.class, "Host not available.");
    }

    @Test
    public void testAddConsumerNodeWhenGetConsumerInfoThrows() {
        MemberInfo memberInfo =
                new MemberInfo("Consumer.01", 0, new ComponentKind[]{ComponentKind.Consumer}, new NodeCapacity());
        ConsumerNode consumerNode = new ConsumerNode(memberInfo);
        doThrow(new RuntimeException("Some unknown failure.")).when(consumerApi).getConsumerInfo();
        RuntimeException re =
                assertThrows(RuntimeException.class, () -> controllerApiMgr.addConsumerNode(consumerNode));
        assertEquals("Some unknown failure.", re.getMessage());
    }

    @Test
    public void testGetSubscriptionStatus() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        assignments.add(getAssignment(sub1, consumerNodes.get(0), shards.get(0)));
        assignments.add(getAssignment(sub1, consumerNodes.get(1), shards.get(1)));
        assignments.add(getAssignment(sub1, consumerNodes.get(2), shards.get(2)));
        doReturn(sub1).when(metaStore).getSubscription(sub1.getName());
        doReturn(assignments).when(assignmentManager).getSubAssignments(sub1.getName());
        setupShardStatus(sub1.getName(), 0, ShardState.STARTED, null);
        setupShardStatus(sub1.getName(), 1, ShardState.STARTED, null);
        setupShardStatus(sub1.getName(), 2, ShardState.STARTED, null);

        CompletableFuture<SubscriptionStatus> result =
                controllerApiMgr.getSubscriptionStatus(sub1.getName(), requestedBy);
        SubscriptionStatus status = awaitAsyncAndGetValue(result);
        assertEquals(SubscriptionState.RUNNING, status.getState());

        setupShardStatus(sub1.getName(), 2, ShardState.ERRORED, "Failed to start shard.");
        result = controllerApiMgr.getSubscriptionStatus(sub1.getName(), requestedBy);
        status = awaitAsyncAndGetValue(result);
        assertEquals(SubscriptionState.ERRORED, status.getState());

        doThrow(new ReplyException(ReplyFailure.NO_HANDLERS, "Host not found.")).when(consumerApi)
                .getShardStatus(sub1.getName(), 0);
        ReplyException re = assertThrows(
                ReplyException.class,
                () -> controllerApiMgr.getSubscriptionStatus(sub1.getName(), requestedBy)
        );
        assertEquals(ReplyFailure.NO_HANDLERS, re.failureType());

        doReturn(CompletableFuture.failedFuture(new RuntimeException("Unknown exception."))).when(consumerApi)
                .getShardStatus(sub1.getName(), 0);
        result = controllerApiMgr.getSubscriptionStatus(sub1.getName(), requestedBy);
        IllegalStateException ie = assertException(result, IllegalStateException.class, null);
        assertTrue(ie.getMessage().contains("Unknown exception."));
        assertTrue(ie.getMessage().contains("Failure in getting subscription status, try again after sometime."));
    }

    @Test
    public void testStartSubscription() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStart(sub1, shards, consumerNodes, assignments, SubscriptionState.STOPPED);
        doReturn(new ArrayList<>()).when(assignmentManager).getSubAssignments(sub1.getName());
        doReturn(CompletableFuture.completedFuture(assignments)).when(assignmentManager)
                .assignShards(shards, sub1, List.of());
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.startSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        verify(assignmentManager, times(1)).assignShards(shards, sub1, List.of());
        verify(consumerApi, times(3)).start(any());
    }

    @Test
    public void testStartOfRunningSubscription() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStart(sub1, shards, consumerNodes, assignments, SubscriptionState.RUNNING);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.startSubscription(sub1.getName(), requestedBy);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        assertException(
                result, InvalidOperationForResourceException.class,
                "Subscription is either already running or starting."
        );
    }

    @Test
    public void testStartOfStartingSubscription() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStart(sub1, shards, consumerNodes, assignments, SubscriptionState.STARTING);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.startSubscription(sub1.getName(), requestedBy);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        assertException(
                result, InvalidOperationForResourceException.class,
                "Subscription is either already running or starting."
        );
    }

    @Test
    public void testStartSubscription_SubscriptionStatusFailure() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();

        setupSubscriptionForStart(sub1, shards, consumerNodes, assignments, SubscriptionState.STOPPED);

        doReturn(CompletableFuture.failedFuture(new MetaStoreException("Failed to get assignments for sub."))).when(
                        controllerApiMgr)
                .getSubscriptionStatus(sub1);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.startSubscription(sub1.getName(), requestedBy);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        IllegalStateException ie = assertException(result, IllegalStateException.class, null);
        assertTrue(ie.getMessage().contains("Failed to get assignments for sub."));
        assertTrue(ie.getMessage().contains("Failure in getting subscription status, try again after sometime"));
    }

    @Test
    public void testStartSubscriptionOneShardAlreadyStarted() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStart(sub1, shards, consumerNodes, assignments, SubscriptionState.STOPPED);

        setupShardStatus(sub1.getName(), 0, ShardState.STARTED, null);
        ArgumentCaptor<ShardOperation.StartData> sCapture = ArgumentCaptor.forClass(ShardOperation.StartData.class);
        doReturn(CompletableFuture.completedFuture(null)).when(consumerApi).start(sCapture.capture());
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.startSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        assertEquals(2, sCapture.getAllValues().size());
        assertEquals(0, sCapture.getAllValues().stream().filter(sd -> sd.getShardId() == 0).count());
    }

    @Test
    public void testStartSubscriptionWhenConsumerStartFails() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStart(sub1, shards, consumerNodes, assignments, SubscriptionState.STOPPED);
        ArgumentCaptor<String> subOpIdCapture = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> shardOpIdCapture = ArgumentCaptor.forClass(String.class);
        List<String> shardOpIds = new ArrayList<>();
        doAnswer(invocationOnMock -> {
            ShardOperation.StartData sd = invocationOnMock.getArgument(0);
            if (sd.getShardId() == 0) {
                shardOpIds.add(sd.getOperationId());
                return CompletableFuture.failedFuture(new ReplyException(ReplyFailure.NO_HANDLERS, "Host not found."));
            }
            return CompletableFuture.completedFuture(null);
        }).when(consumerApi).start(any());

        doNothing().when(operationMgr)
                .updateShardOp(anyString(), anyString(), any(ShardOperation.State.class), anyString());
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.startSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        verify(operationMgr, times(1)).updateShardOp(
                anyString(), anyString(), any(ShardOperation.State.class), anyString());
        verify(operationMgr).updateShardOp(subOpIdCapture.capture(), shardOpIdCapture.capture(), any(), anyString());
        assertEquals(op.getId(), subOpIdCapture.getValue());
        assertEquals(shardOpIds.get(0), shardOpIdCapture.getValue());
    }

    @Test
    public void testStartSubscriptionWhenConsumerStartShardThrows() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStart(sub1, shards, consumerNodes, assignments, SubscriptionState.STOPPED);
        doAnswer(invocationOnMock -> {
            ShardOperation.StartData sd = invocationOnMock.getArgument(0);
            if (sd.getShardId() == 0) {
                throw new ReplyException(ReplyFailure.NO_HANDLERS, "Host not found.");
            }
            return CompletableFuture.completedFuture(null);
        }).when(consumerApi).start(any());
        doNothing().when(operationMgr)
                .updateShardOp(anyString(), anyString(), any(ShardOperation.State.class), anyString());

        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.startSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        verify(operationMgr, times(1)).updateShardOp(
                anyString(), anyString(), any(ShardOperation.State.class), anyString());
    }

    @Test
    public void testStopSubscriptionAssignmentExists() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();

        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.RUNNING);
        doReturn(CompletableFuture.completedFuture(null)).when(assignmentManager).unAssignShards(assignments, sub1, true);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        verify(consumerApi, times(3)).stop(any());
        verify(assignmentManager, times(1)).unAssignShards(assignments, sub1, true);
    }

//    @Test
//    public void testStopSubscriptionAssignmentDoesNotExists() {
//        //ideally shouldn't happen as Status would return appropriate state, but to ensure code path doesn't fail for this case
//        VaradhiSubscription sub1 =
//                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
//        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
//        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
//        List<Assignment> assignments = new ArrayList<>();
//        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.ERRORED);
//
//        doReturn(CompletableFuture.completedFuture(null)).when(assignmentManager)
//                .unAssignShards(new ArrayList<>(), sub1, true);
//
//        doReturn(new ArrayList<>()).when(assignmentManager).getSubAssignments(sub1.getName());
//        doReturn(CompletableFuture.completedFuture(assignments)).when(assignmentManager)
//                .assignShards(shards, sub1, List.of());
//        CompletableFuture<SubscriptionOperation> result =
//                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
//        SubscriptionOperation op = awaitAsyncAndGetValue(result);
//        // since no assignment, op gets completed on same thread.
//        assertEquals(SubscriptionOperation.State.IN_PROGRESS, op.getState());
//        verify(assignmentManager, times(1)).unAssignShards(new ArrayList<>(), sub1, true);
//        verify(consumerApi, times(0)).stop(any());
//    }

    @Test
    public void testStopOfStoppedSubscription() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.STOPPED);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        assertException(
                result, InvalidOperationForResourceException.class,
                "Subscription is either already stopped or stopping."
        );
    }

    @Test
    public void testStartOfStoppingSubscription() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.STOPPING);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        assertException(
                result, InvalidOperationForResourceException.class,
                "Subscription is either already stopped or stopping."
        );
    }

    @Test
    public void testStopSubscription_SubscriptionStatusFailure() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();

        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.RUNNING);

        doReturn(CompletableFuture.failedFuture(new MetaStoreException("Failed to get assignments for sub."))).when(
                        controllerApiMgr)
                .getSubscriptionStatus(sub1);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
        await().atMost(100, TimeUnit.SECONDS).until(result::isDone);
        IllegalStateException ie = assertException(result, IllegalStateException.class, null);
        assertTrue(ie.getMessage().contains("Failed to get assignments for sub."));
        assertTrue(ie.getMessage().contains("Failure in getting subscription status, try again after sometime"));
    }

    @Test
    public void testStopSubscriptionOneShardAlreadyStopped() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.ERRORED);

        setupShardStatus(sub1.getName(), 0, ShardState.UNKNOWN, null);
        ArgumentCaptor<ShardOperation.StopData> sCapture = ArgumentCaptor.forClass(ShardOperation.StopData.class);
        doReturn(CompletableFuture.completedFuture(null)).when(consumerApi).stop(sCapture.capture());
        doReturn(CompletableFuture.completedFuture(null)).when(assignmentManager).unAssignShards(assignments, sub1, true);
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        assertEquals(2, sCapture.getAllValues().size());
        assertEquals(0, sCapture.getAllValues().stream().filter(sd -> sd.getShardId() == 0).count());
        verify(assignmentManager, times(1)).unAssignShards(assignments, sub1, true);
    }

    @Test
    public void testStopSubscriptionWhenConsumerStopFails() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.RUNNING);
        doAnswer(invocationOnMock -> {
            ShardOperation.StopData sd = invocationOnMock.getArgument(0);
            if (sd.getShardId() == 0) {
                return CompletableFuture.failedFuture(new ReplyException(ReplyFailure.NO_HANDLERS, "Host not found."));
            }
            return CompletableFuture.completedFuture(null);
        }).when(consumerApi).stop(any());
        doReturn(CompletableFuture.completedFuture(null)).when(assignmentManager).unAssignShards(assignments, sub1, true);

        doNothing().when(operationMgr)
                .updateShardOp(anyString(), anyString(), any(ShardOperation.State.class), anyString());
        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        verify(operationMgr, times(1)).updateShardOp(
                anyString(), anyString(), any(ShardOperation.State.class), anyString());
        verify(assignmentManager, times(1)).unAssignShards(assignments, sub1, true);
    }

    @Test
    public void testStopSubscriptionWhenConsumerStopShardThrows() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> consumerNodes = getConsumerNodes(3);
        List<Assignment> assignments = new ArrayList<>();
        setupSubscriptionForStop(sub1, shards, consumerNodes, assignments, SubscriptionState.RUNNING);
        doAnswer(invocationOnMock -> {
            ShardOperation.StopData sd = invocationOnMock.getArgument(0);
            if (sd.getShardId() == 0) {
                throw new ReplyException(ReplyFailure.NO_HANDLERS, "Host not found.");
            }
            return CompletableFuture.completedFuture(null);
        }).when(consumerApi).stop(any());
        doNothing().when(operationMgr)
                .updateShardOp(anyString(), anyString(), any(ShardOperation.State.class), anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(assignmentManager).unAssignShards(assignments, sub1, true);

        CompletableFuture<SubscriptionOperation> result =
                controllerApiMgr.stopSubscription(sub1.getName(), requestedBy);
        SubscriptionOperation op = awaitAsyncAndGetValue(result);
        validateOpQueued(op);
        verify(operationMgr, times(1)).updateShardOp(
                anyString(), anyString(), any(ShardOperation.State.class), anyString());
        verify(assignmentManager, times(1)).unAssignShards(assignments, sub1, true);
    }

    @Test
    public void testUpdateShardOp() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        SubscriptionOperation subOp = OperationMgrTest.getStartOp(sub1);
        ShardOperation shardOp = OperationMgrTest.getShardStartOp(subOp.getId(), shards.get(0), sub1);
        doNothing().when(opStore).updateShardOp(any());
        controllerApiMgr.update(subOp.getId(), shardOp.getId(), shardOp.getState(), shardOp.getErrorMsg());
        verify(operationMgr, times(1)).updateShardOp(
                subOp.getId(), shardOp.getId(), shardOp.getState(), shardOp.getErrorMsg());
    }

    @Test
    public void testGetAllAssignments() {
        List<Assignment> expectedAssignments = new ArrayList<>();
        when(assignmentManager.getAllAssignments()).thenReturn(expectedAssignments);
        List<Assignment> result = controllerApiMgr.getAllAssignments();
        verify(assignmentManager, times(1)).getAllAssignments();
        assertListEquals(expectedAssignments, result);
    }

    @Test
    public void testGetPendingSubOps() {
        controllerApiMgr.getPendingSubOps();
        verify(operationMgr, times(1)).getPendingSubOps();
    }

    @Test
    public void testRetryOperation() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation subOp = OperationMgrTest.getStartOp(sub1);
        doReturn(sub1).when(metaStore).getSubscription(sub1.getName());
        doNothing().when(operationMgr).enqueue(any(), any());
        controllerApiMgr.retryOperation(subOp);
        verify(operationMgr, times(1)).enqueue(eq(subOp), any());
    }

    @Test
    public void testConsumerNodeJoined() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(3).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        ConsumerNode node = NodeProvider.getConsumerNodes(1).get(0);
        doReturn(CompletableFuture.completedFuture(NodeProvider.getConsumerInfo(node, sub1, shards))).when(consumerApi)
                .getConsumerInfo();
        doReturn(CompletableFuture.completedFuture(null)).when(assignmentManager).consumerNodeJoined(node);
        CompletableFuture<Void> result = controllerApiMgr.consumerNodeJoined(node);
        awaitAsyncAndGetValue(result);
        verify(consumerApi, times(1)).getConsumerInfo();
        verify(assignmentManager, times(1)).consumerNodeJoined(node);
    }

    @Test
    public void testConsumerNodeLeft() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards1 = SubProvider.shardsOf(sub1);
        VaradhiSubscription sub2 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub2", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards2 = SubProvider.shardsOf(sub2);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(3);

        List<Assignment> consumer0Assignments = new ArrayList<>();
        consumer0Assignments.add(getAssignment(sub1, nodes.get(0), shards1.get(0)));
        consumer0Assignments.add(getAssignment(sub2, nodes.get(0), shards2.get(0)));
        doAnswer(invocationOnMock -> new HashMap<>()).when(operationMgr).getShardOps(any());

        doReturn(CompletableFuture.completedFuture(getAssignment(sub1, nodes.get(1), shards1.get(0)))).when(
                        assignmentManager)
                .reAssignShard(consumer0Assignments.get(0), sub1, false);
        doReturn(CompletableFuture.completedFuture(getAssignment(sub2, nodes.get(1), shards2.get(0)))).when(
                        assignmentManager)
                .reAssignShard(consumer0Assignments.get(1), sub2, false);

        doReturn(sub1).when(metaStore).getSubscription(sub1.getName());
        doReturn(sub2).when(metaStore).getSubscription(sub2.getName());
        doReturn(consumer0Assignments).when(assignmentManager).getConsumerNodeAssignments(nodes.get(0).getConsumerId());

        doReturn(CompletableFuture.completedFuture(null)).when(assignmentManager)
                .consumerNodeLeft(nodes.get(0).getConsumerId());

        setupSubscriptionForOp(
                sub1, List.of(shards1.get(0)), List.of(nodes.get(1)), new ArrayList<>(), ShardState.UNKNOWN,
                SubscriptionState.RUNNING
        );
        setupSubscriptionForOp(
                sub2, List.of(shards2.get(0)), List.of(nodes.get(1)), new ArrayList<>(), ShardState.UNKNOWN,
                SubscriptionState.RUNNING
        );

        CountDownLatch shardStartLatch = new CountDownLatch(2);
        doAnswer(invocationOnMock -> {
            shardStartLatch.countDown();
            return CompletableFuture.completedFuture(null);
        }).when(consumerApi).start(any());

        CompletableFuture<Void> result = controllerApiMgr.consumerNodeLeft(nodes.get(0).getConsumerId());
        awaitAsyncAndGetValue(result);
        await().atMost(20, TimeUnit.SECONDS).until(() -> shardStartLatch.getCount() == 0);

        verify(assignmentManager, times(1)).consumerNodeLeft(nodes.get(0).getConsumerId());
        verify(operationMgr, times(2)).createAndEnqueue(any(), any());
        verify(consumerApi, times(2)).start(any());
    }

    private void setupSubscriptionForStart(
            VaradhiSubscription sub1, List<SubscriptionUnitShard> shards, List<ConsumerNode> consumerNodes,
            List<Assignment> assignments, SubscriptionState state
    ) {
        setupSubscriptionForOp(sub1, shards, consumerNodes, assignments, ShardState.UNKNOWN, state);
        doReturn(CompletableFuture.completedFuture(null)).when(consumerApi).start(any());
    }

    private void setupSubscriptionForStop(
            VaradhiSubscription sub1, List<SubscriptionUnitShard> shards, List<ConsumerNode> consumerNodes,
            List<Assignment> assignments, SubscriptionState state
    ) {
        setupSubscriptionForOp(sub1, shards, consumerNodes, assignments, ShardState.STARTED, state);
        doReturn(CompletableFuture.completedFuture(null)).when(consumerApi).stop(any());
    }

    private void setupSubscriptionForOp(
            VaradhiSubscription sub1, List<SubscriptionUnitShard> shards, List<ConsumerNode> consumerNodes,
            List<Assignment> assignments, ShardState shardState, SubscriptionState state
    ) {
        for (int i = 0; i < shards.size(); i++) {
            assignments.add(getAssignment(sub1, consumerNodes.get(i), shards.get(i)));
            setupShardStatus(sub1.getName(), shards.get(i).getShardId(), shardState, null);
        }
        doReturn(sub1).when(metaStore).getSubscription(sub1.getName());
        doReturn(assignments).when(assignmentManager).getSubAssignments(sub1.getName());
        doReturn(CompletableFuture.completedFuture(assignments)).when(assignmentManager)
                .assignShards(shards, sub1, List.of());
        doReturn(true).when(opStore).shardOpExists(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(consumerApi).stop(any());
        doReturn(new HashMap<>()).when(operationMgr).getShardOps(sub1.getName());
        doReturn(CompletableFuture.completedFuture(
                new SubscriptionStatus(sub1.getName(), state))).when(controllerApiMgr)
                .getSubscriptionStatus(sub1);
    }

    private void setupShardStatus(String subscriptionId, int shardId, ShardState state, String failureReason) {
        ShardStatus status = new ShardStatus(state, failureReason);
        doReturn(CompletableFuture.completedFuture(status)).when(consumerApi).getShardStatus(subscriptionId, shardId);
    }

    private void validateOpQueued(SubscriptionOperation subOp) {
        assertEquals(SubscriptionOperation.State.IN_PROGRESS, subOp.getState());
    }


}
