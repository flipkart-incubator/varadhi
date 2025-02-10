package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.config.ControllerConfig;
import com.flipkart.varadhi.entities.SubscriptionUtils;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.ShardOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.OpStore;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.*;

import static com.flipkart.varadhi.entities.cluster.Operation.State.ERRORED;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class OperationMgrTest {
    private final int MAX_CONCURRENT_OPS = 2;
    @Mock
    private OpStore opStore;
    @Mock
    private ControllerConfig config;
    private OperationMgr operationMgr;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        doReturn(MAX_CONCURRENT_OPS).when(config).getMaxConcurrentOps();
        operationMgr = new OperationMgr(config.getMaxConcurrentOps(), opStore, new RetryPolicy(0, 4, 5, 20));
        executor = Executors.newCachedThreadPool();
    }

    @Test
    public void scheduleAndExecuteOperation() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.completedFuture(null);
        });

        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        assertTrue(executionCalled.isDone());
        executionLatch.countDown();
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed = completeOperation(startOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
    }

    @Test
    public void orderedExecutionForSingleSubscription() {
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp1 = getStartOp(sub1);
        SubscriptionOperation stopOp1 = getStopOp(sub1);
        SubscriptionOperation startOp2 = getStartOp(sub1);
        String orderingKey1 = stopOp1.getOrderingKey();

        CompletableFuture<Void> execution1Called = new CompletableFuture<>();
        CompletableFuture<Void> execution2Called = new CompletableFuture<>();
        CompletableFuture<Void> execution3Called = new CompletableFuture<>();
        operationMgr.enqueue(startOp1, operation -> {
            execution1Called.complete(null);
            return CompletableFuture.completedFuture(null);
        });
        operationMgr.enqueue(stopOp1, operation -> {
            execution2Called.complete(null);
            return CompletableFuture.completedFuture(null);
        });
        operationMgr.enqueue(startOp2, operation -> {
            execution3Called.complete(null);
            return CompletableFuture.completedFuture(null);
        });

        assertEquals(3, operationMgr.getPendingOperations(orderingKey1).size());
        await().atMost(100, TimeUnit.SECONDS).until(execution1Called::isDone);
        assertTrue(execution1Called.isDone());
        assertFalse(execution2Called.isDone());
        assertFalse(execution3Called.isDone());
        CountDownLatch completed1 = completeOperation(startOp1);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed1.getCount() == 0);
        assertEquals(2, operationMgr.getPendingOperations(orderingKey1).size());
        assertTrue(execution2Called.isDone());
        assertFalse(execution3Called.isDone());
        CountDownLatch completed2 = completeOperation(stopOp1);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed2.getCount() == 0);
        assertEquals(1, operationMgr.getPendingOperations(orderingKey1).size());
        assertTrue(execution3Called.isDone());
        CountDownLatch completed3 = completeOperation(startOp2);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed3.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(orderingKey1).size());
    }

    @Test
    public void duplicateOperationsAreIgnored() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.completedFuture(null);
        });

        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.completedFuture(null);
        });

        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        assertTrue(executionCalled.isDone());
        executionLatch.countDown();
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        operationMgr.enqueue(startOp, operation -> CompletableFuture.runAsync(() -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
        }, executor));
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed = completeOperation(startOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
    }

    @Test
    public void parallelExecutionForDifferentSubs() {
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        VaradhiSubscription sub2 = SubscriptionUtils.builder().build("project1.sub2", "project1", "project1.topic1");
        VaradhiSubscription sub3 = SubscriptionUtils.builder().build("project1.sub3", "project1", "project1.topic1");
        SubscriptionOperation startOp1 = getStartOp(sub1);
        SubscriptionOperation startOp2 = getStopOp(sub2);
        SubscriptionOperation startOp3 = getStartOp(sub3);
        String orderingKey1 = startOp1.getOrderingKey();
        String orderingKey2 = startOp2.getOrderingKey();
        String orderingKey3 = startOp3.getOrderingKey();
        CountDownLatch waiting1 = new CountDownLatch(1);
        CountDownLatch waiting2 = new CountDownLatch(1);
        CountDownLatch waiting3 = new CountDownLatch(1);
        CompletableFuture<Void> execution1Called = new CompletableFuture<>();
        CompletableFuture<Void> execution2Called = new CompletableFuture<>();
        CompletableFuture<Void> execution3Called = new CompletableFuture<>();
        operationMgr.enqueue(startOp1, operation -> {
            execution1Called.complete(null);
            waitForExecution(waiting1);
            return CompletableFuture.completedFuture(null);
        });
        operationMgr.enqueue(startOp2, operation -> {
            execution2Called.complete(null);
            waitForExecution(waiting2);
            return CompletableFuture.completedFuture(null);
        });
        operationMgr.enqueue(startOp3, operation -> {
            execution3Called.complete(null);
            waitForExecution(waiting3);
            return CompletableFuture.completedFuture(null);
        });

        assertEquals(1, operationMgr.getPendingOperations(orderingKey1).size());
        assertEquals(1, operationMgr.getPendingOperations(orderingKey2).size());
        assertEquals(1, operationMgr.getPendingOperations(orderingKey3).size());
        await().atMost(10, TimeUnit.SECONDS).until(() -> execution1Called.isDone() && execution2Called.isDone());
        assertTrue(execution1Called.isDone());
        assertTrue(execution2Called.isDone());
        assertFalse(execution3Called.isDone());

        waiting1.countDown();
        CountDownLatch completed1 = completeOperation(startOp1);
        await().atMost(20, TimeUnit.SECONDS).until(() -> completed1.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(orderingKey1).size());
        assertEquals(1, operationMgr.getPendingOperations(orderingKey2).size());
        assertEquals(1, operationMgr.getPendingOperations(orderingKey3).size());
        assertTrue(execution3Called.isDone());

        waiting2.countDown();
        CountDownLatch completed2 = completeOperation(startOp2);
        waiting3.countDown();
        CountDownLatch completed3 = completeOperation(startOp3);
        await().atMost(1, TimeUnit.SECONDS).until(() -> completed2.getCount() == 0 && completed3.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(orderingKey2).size());
        assertEquals(0, operationMgr.getPendingOperations(orderingKey3).size());
    }


    @Test
    public void updateOnNonTrackedTaskIsIgnored() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        VaradhiSubscription sub2 = SubscriptionUtils.builder().build("project1.sub2", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        SubscriptionOperation startOp2 = getStartOp(sub1);
        SubscriptionOperation startOp3 = getStartOp(sub2);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.completedFuture(null);
        });

        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        assertTrue(executionCalled.isDone());
        executionLatch.countDown();
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed = completeOperation(startOp2);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed.getCount() == 0);
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed2 = completeOperation(startOp3);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed2.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(startOp3.getOrderingKey()).size());
    }


    @Test
    public void partialUpdatesDoesNotCompleteTask() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.completedFuture(null);
        });

        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        assertTrue(executionCalled.isDone());
        executionLatch.countDown();
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed = updateOperation(startOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed.getCount() == 0);
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed2 = completeOperation(startOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed2.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
    }

    @Test
    public void updateWaitingTaskIsIgnored() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        SubscriptionOperation stopOp = getStopOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.completedFuture(null);
        });

        operationMgr.enqueue(stopOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.completedFuture(null);
        });
        executionLatch.countDown();
        assertEquals(2, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed = completeOperation(stopOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed.getCount() == 0);
        assertEquals(2, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed2 = completeOperation(startOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed2.getCount() == 0);
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());

        CountDownLatch completed3 = completeOperation(stopOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed3.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
    }

    @Test
    public void enqueueWhenOpExecutorThrows() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        ArgumentCaptor<SubscriptionOperation> opCaptor = ArgumentCaptor.forClass(SubscriptionOperation.class);
        CountDownLatch updateLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            updateLatch.countDown();
            return null;
        }).when(opStore).updateSubOp(opCaptor.capture());

        operationMgr.enqueue(startOp, operation -> {
            waitForExecution(executionLatch);
            throw new MetaStoreException("Failed to allocate.");
        });

        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        executionLatch.countDown();
        await().atMost(100, TimeUnit.SECONDS).until(() -> updateLatch.getCount() == 0);

        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        validateOp(opCaptor.getValue(), startOp.getId(), ERRORED,"Failed to allocate.");
    }

    @Test
    public void enqueueWhenOpFailsWithException() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        ArgumentCaptor<SubscriptionOperation> opCaptor = ArgumentCaptor.forClass(SubscriptionOperation.class);
        CountDownLatch updateLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            updateLatch.countDown();
            return null;
        }).when(opStore).updateSubOp(opCaptor.capture());

        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.failedFuture(new MetaStoreException("Failed to allocate."));
        });

        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        executionLatch.countDown();
        await().atMost(100, TimeUnit.SECONDS).until(() -> updateLatch.getCount() == 0);

        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        validateOp(opCaptor.getValue(), startOp.getId(), ERRORED,"Failed to allocate.");
    }

    @Test
    public void updateOpThrows() {
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        ArgumentCaptor<SubscriptionOperation> opCaptor = ArgumentCaptor.forClass(SubscriptionOperation.class);
        MutableBoolean firstCall = new MutableBoolean(true);
        doAnswer(invocation -> {
            // first called from updateSubOp, in case of failure it will be called again from saveFailure in OpTask.
            if (firstCall.booleanValue()) {
                firstCall.setFalse();
                throw new MetaStoreException("Failed to update in handler");
            }
            return null;
        }).when(opStore).updateSubOp(opCaptor.capture());

        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            return CompletableFuture.completedFuture(null);
        });
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        completeOperation(startOp);

        await().atMost(100, TimeUnit.SECONDS)
                .until(() -> operationMgr.getPendingOperations(startOp.getOrderingKey()).isEmpty());
        validateOp(opCaptor.getValue(), startOp.getId(), ERRORED,"Failed to update in handler");
    }

    @Test
    public void saveFailureInExecutionRemovesPendingTask() {
        CountDownLatch executionLatch = new CountDownLatch(1);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        ArgumentCaptor<SubscriptionOperation> opCaptor = ArgumentCaptor.forClass(SubscriptionOperation.class);
        doAnswer(invocation -> {
            throw new MetaStoreException("Failed to update");
        }).when(opStore).updateSubOp(any());

        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            waitForExecution(executionLatch);
            return CompletableFuture.failedFuture(new MetaStoreException("Failed to allocate."));
        });

        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        executionLatch.countDown();
        await().atMost(100, TimeUnit.SECONDS)
                .until(() -> operationMgr.getPendingOperations(startOp.getOrderingKey()).isEmpty());
        verify(opStore).updateSubOp(opCaptor.capture());
        validateOp(opCaptor.getValue(), startOp.getId(), ERRORED,"Failed to allocate.");
    }

    @Test
    public void saveFailureInUpdateRemovesPendingTask() {
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        CompletableFuture<Void> executionCalled = new CompletableFuture<>();
        ArgumentCaptor<SubscriptionOperation> opCaptor = ArgumentCaptor.forClass(SubscriptionOperation.class);
        doThrow(new MetaStoreException("Failed to update in handler"), new MetaStoreException("Failed to update in saveFailure")).when(opStore).updateSubOp(opCaptor.capture());

        operationMgr.enqueue(startOp, operation -> {
            executionCalled.complete(null);
            return CompletableFuture.completedFuture(null);
        });
        await().atMost(100, TimeUnit.SECONDS).until(executionCalled::isDone);
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        completeOperation(startOp);

        await().atMost(100, TimeUnit.SECONDS)
                .until(() -> operationMgr.getPendingOperations(startOp.getOrderingKey()).isEmpty());
        validateOp(opCaptor.getValue(), startOp.getId(), ERRORED,"Failed to update in handler");
    }

    @Test
    public void updateOfShardOpUpdatesSubscriptionOp() {
        VaradhiSubscription sub1 =
                SubscriptionUtils.builder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubscriptionUtils.shardsOf(sub1);
        SubscriptionOperation startSubOp = getStartOp(sub1);
        ShardOperation shard1Op = getShardStartOp(startSubOp.getId(), shards.get(0), sub1);
        ShardOperation shard2Op = getShardStartOp(startSubOp.getId(), shards.get(1), sub1);

        operationMgr.enqueue(startSubOp, operation -> CompletableFuture.completedFuture(null));
        assertEquals(1, operationMgr.getPendingOperations(startSubOp.getOrderingKey()).size());

        doReturn(startSubOp).when(opStore).getSubOp(startSubOp.getId());
        doReturn(shard1Op).when(opStore).getShardOp(shard1Op.getId());
        doReturn(shard2Op).when(opStore).getShardOp(shard2Op.getId());
        doReturn(List.of(shard1Op, shard2Op)).when(opStore).getShardOps(startSubOp.getId());

        CountDownLatch shard1Latch = completeOperation(shard1Op);
        await().atMost(100, TimeUnit.SECONDS).until(() -> shard1Latch.getCount() == 0);
        assertEquals(1, operationMgr.getPendingOperations(startSubOp.getOrderingKey()).size());

        CountDownLatch shard2Latch = completeOperation(shard2Op);
        await().atMost(100, TimeUnit.SECONDS).until(() -> shard2Latch.getCount() == 0);

        await().atMost(100, TimeUnit.SECONDS)
                .until(() -> operationMgr.getPendingOperations(startSubOp.getOrderingKey()).isEmpty());
        verify(opStore, times(2)).updateSubOp(startSubOp);
        verify(opStore, times(1)).updateShardOp(shard1Op);
        verify(opStore, times(1)).updateShardOp(shard2Op);
    }

    @Test
    public void testCreateAndEnqueue() {
        VaradhiSubscription sub1 =
                SubscriptionUtils.builder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startSubOp = getStartOp(sub1);
        operationMgr.createAndEnqueue(startSubOp, operation -> CompletableFuture.completedFuture(null));
        verify(opStore, times(1)).createSubOp(startSubOp);
    }

    @Test
    public void testCreateIfNeededAndExecute() {
        VaradhiSubscription sub1 =
                SubscriptionUtils.builder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubscriptionUtils.shardsOf(sub1);
        SubscriptionOperation startSubOp = getStartOp(sub1);
        ShardOperation shard1Op = getShardStartOp(startSubOp.getId(), shards.get(0), sub1);
        ShardOperation shard2Op = getShardStartOp(startSubOp.getId(), shards.get(1), sub1);
        doReturn(false).when(opStore).shardOpExists(shard1Op.getId());
        doReturn(true).when(opStore).shardOpExists(shard2Op.getId());

        operationMgr.submitShardOp(shard1Op, false);
        verify(opStore, times(1)).createShardOp(shard1Op);
        operationMgr.submitShardOp(shard2Op, false);
        verify(opStore, never()).createShardOp(shard2Op);
    }

    @Test
    public void failedOperationShouldBeRetried() {
        RetryPolicy retryPolicy = new RetryPolicy(1, 1, 1, 1);
        operationMgr = new OperationMgr(config.getMaxConcurrentOps(), opStore, retryPolicy);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);

        operationMgr.enqueue(startOp, operation -> CompletableFuture.completedFuture(null));

        CountDownLatch completed1 = failOperation(startOp, new MetaStoreException("Some failure."));
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed1.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        OperationMgr.RetryOpTask retryOpTask =  operationMgr.getRetryOperations(startOp.getOrderingKey());
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        assertNotNull(retryOpTask);
        assertEquals(1, retryOpTask.opTask.operation.getRetryAttempt());

        // retry should happen in a second.
        await().atMost(2, TimeUnit.SECONDS).until(() -> operationMgr.getPendingOperations(startOp.getOrderingKey()).size() == 1);
        CountDownLatch completed2 = completeOperation(startOp);
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed2.getCount() == 0);
        retryOpTask =  operationMgr.getRetryOperations(startOp.getOrderingKey());
        // successful operation shouldn't be retried.
        assertNull(retryOpTask);
    }

    @Test
    public void failedOperationIsNotRetriedIfSubAlreadyHasPendingOp() {
        RetryPolicy retryPolicy = new RetryPolicy(1, 1, 1, 1);
        operationMgr = new OperationMgr(config.getMaxConcurrentOps(), opStore, retryPolicy);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        SubscriptionOperation stopOp = getStopOp(sub1);

        operationMgr.enqueue(startOp, operation -> CompletableFuture.completedFuture(null));
        operationMgr.enqueue(stopOp, operation -> CompletableFuture.completedFuture(null));

        assertEquals(2, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        CountDownLatch completed1 = failOperation(startOp, new MetaStoreException("Some failure."));
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed1.getCount() == 0);
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        OperationMgr.RetryOpTask retryOpTask =  operationMgr.getRetryOperations(startOp.getOrderingKey());
        assertNull(retryOpTask);
    }

    @Test
    public void subsequentOperationShouldClearPendingRetriesIfAny() {
        RetryPolicy retryPolicy = new RetryPolicy(1, 1, 1, 1);
        operationMgr = new OperationMgr(config.getMaxConcurrentOps(), opStore, retryPolicy);
        VaradhiSubscription sub1 = SubscriptionUtils.builder().build("project1.sub1", "project1", "project1.topic1");
        SubscriptionOperation startOp = getStartOp(sub1);
        SubscriptionOperation stopOp = getStopOp(sub1);

        operationMgr.enqueue(startOp, operation -> CompletableFuture.completedFuture(null));

        CountDownLatch completed1 = failOperation(startOp, new MetaStoreException("Some failure."));
        await().atMost(100, TimeUnit.SECONDS).until(() -> completed1.getCount() == 0);
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        OperationMgr.RetryOpTask retryOpTask =  operationMgr.getRetryOperations(startOp.getOrderingKey());
        assertEquals(0, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
        assertNotNull(retryOpTask);
        operationMgr.enqueue(stopOp, operation -> CompletableFuture.completedFuture(null));
        retryOpTask =  operationMgr.getRetryOperations(startOp.getOrderingKey());
        assertNull(retryOpTask);
        assertEquals(1, operationMgr.getPendingOperations(startOp.getOrderingKey()).size());
    }


    private void waitForExecution(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private CountDownLatch completeOperation(SubscriptionOperation operation) {
        operation.markCompleted();
        return updateOperation(operation);
    }

    private CountDownLatch failOperation(SubscriptionOperation operation, Exception e) {
        operation.markFail(e.getMessage());
        return updateOperation(operation);
    }

    private CountDownLatch updateOperation(SubscriptionOperation operation) {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture.runAsync(() -> {
            doReturn(operation).when(opStore).getSubOp(operation.getId());
            operationMgr.updateSubOp(operation);
            latch.countDown();
        }, executor);
        return latch;
    }

    private CountDownLatch completeOperation(ShardOperation shardOp) {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture.runAsync(() -> {
            shardOp.markCompleted();
            operationMgr.updateShardOp(shardOp.getOpData().getParentOpId(), shardOp.getId(), shardOp.getState(), shardOp.getErrorMsg());
            latch.countDown();
        }, executor);
        return latch;
    }

    private void validateOp(SubscriptionOperation op, String opId, SubscriptionOperation.State state, String errorMsg) {
        assertEquals(opId, op.getId());
        assertEquals(state, op.getState());
        assertEquals(errorMsg, op.getErrorMsg());
    }

    public static SubscriptionOperation getStartOp(VaradhiSubscription subscription) {
        return SubscriptionOperation.startOp(subscription.getName(), "Anonymous");
    }

    public static SubscriptionOperation getStopOp(VaradhiSubscription subscription) {
        return SubscriptionOperation.stopOp(subscription.getName(), "Anonymous");
    }

    public static ShardOperation getShardStartOp(String subOpId, SubscriptionUnitShard shard, VaradhiSubscription sub) {
        return ShardOperation.startOp(subOpId, shard, sub);
    }
}
