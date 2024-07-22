package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.controller.impl.LeastAssignedStrategy;
import com.flipkart.varadhi.entities.NodeProvider;
import com.flipkart.varadhi.entities.SubProvider;
import com.flipkart.varadhi.entities.SubscriptionUnitShard;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.cluster.Assignment;
import com.flipkart.varadhi.entities.cluster.ConsumerNode;
import com.flipkart.varadhi.entities.cluster.NodeCapacity;
import com.flipkart.varadhi.exceptions.CapacityException;
import com.flipkart.varadhi.spi.db.AssignmentStore;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.flipkart.varadhi.TestHelper.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AssignmentManagerTest {
    @Mock
    private AssignmentStore assignmentStore;

    private AssignmentStrategy strategy;
    @Mock
    private MeterRegistry meterRegistry;
    private AssignmentManager assignmentManager;


    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        strategy = spy(new LeastAssignedStrategy());
        assignmentManager = spy(new AssignmentManager(strategy, assignmentStore, meterRegistry));
    }

    @Test
    public void testAssignShards() throws Exception {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(3);
        nodes.forEach(this::addConsumerNode);
        NodeCapacity c1 = nodes.get(1).getAvailable().clone();
        ArgumentCaptor<List<Assignment>> assignmentCapture = ArgumentCaptor.forClass(List.class);

        doNothing().when(assignmentStore).createAssignments(assignmentCapture.capture());
        String consumerIdToExclude = nodes.get(1).getConsumerId();
        List<String> idsToExclude = List.of(consumerIdToExclude);

        doReturn(new ArrayList<>()).when(assignmentStore).getSubAssignments(sub1.getName());
        CompletableFuture<List<Assignment>> aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        List<Assignment> assignments = aFuture.get();
        assertEquals(shards.size(), assignments.size());
        assignments.forEach(a -> assertNotEquals(consumerIdToExclude, a.getConsumerId()));
        assertEquals(c1, nodes.get(1).getAvailable());
        assertListEquals(assignments, assignmentCapture.getValue());

        //already assigned should be ignored.
        NodeCapacity c0 = nodes.get(0).getAvailable().clone();
        NodeCapacity c2 = nodes.get(2).getAvailable().clone();
        doReturn(assignments).when(assignmentStore).getSubAssignments(sub1.getName());
        doNothing().when(assignmentStore).createAssignments(assignmentCapture.capture());
        CompletableFuture<List<Assignment>> aFutureNext = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFutureNext::isDone);
        assertListEquals(assignments, aFutureNext.get());
        validateCapacity(c0, c1, c2, nodes);
        assertEquals(2, assignmentCapture.getAllValues().size());
        assertEquals(0, assignmentCapture.getAllValues().get(1).size());
    }

    @Test
    public void testAssignShards_StoreThrows() {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(3);
        nodes.forEach(this::addConsumerNode);
        NodeCapacity c0 = nodes.get(0).getAvailable().clone();
        NodeCapacity c1 = nodes.get(1).getAvailable().clone();
        NodeCapacity c2 = nodes.get(2).getAvailable().clone();
        List<String> idsToExclude = List.of(nodes.get(1).getConsumerId());

        doThrow(new MetaStoreException("failed to get assignments.")).when(assignmentStore)
                .getSubAssignments(sub1.getName());
        CompletableFuture<List<Assignment>> aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        assertException(aFuture, MetaStoreException.class, "failed to get assignments.");
        verify(assignmentStore, times(0)).createAssignments(anyList());
        validateCapacity(c0, c1, c2, nodes);


        doReturn(new ArrayList<>()).when(assignmentStore).getSubAssignments(sub1.getName());
        doThrow(new MetaStoreException("failed to create assignments.")).when(assignmentStore)
                .createAssignments(anyList());
        aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        assertException(aFuture, MetaStoreException.class, "failed to create assignments.");
        validateCapacity(c0, c1, c2, nodes);

        doReturn(new ArrayList<>()).when(assignmentStore).getSubAssignments(sub1.getName());
        doThrow(new CapacityException("Not enough capacity.")).when(strategy).assign(anyList(), any(), anyList());
        doNothing().when(assignmentStore).createAssignments(anyList());
        aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        assertException(aFuture, CapacityException.class, "Not enough capacity.");
        validateCapacity(c0, c1, c2, nodes);
    }

    @Test
    public void testUnAssignShards() throws Exception {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(3);
        nodes.forEach(this::addConsumerNode);
        NodeCapacity c0 = nodes.get(0).getAvailable().clone();
        NodeCapacity c1 = nodes.get(1).getAvailable().clone();
        NodeCapacity c2 = nodes.get(2).getAvailable().clone();
        List<String> idsToExclude = List.of(nodes.get(1).getConsumerId());

        CompletableFuture<List<Assignment>> aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        List<Assignment> assignments = aFuture.get();
        verify(assignmentStore, times(1)).getSubAssignments(sub1.getName());
        ArgumentCaptor<List<Assignment>> assignmentCapture = ArgumentCaptor.forClass(List.class);
        doReturn(assignments).when(assignmentStore).getSubAssignments(sub1.getName());
        doNothing().when(assignmentStore).deleteAssignments(assignmentCapture.capture());
        CompletableFuture<Void> vFuture = assignmentManager.unAssignShards(assignments, sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(vFuture::isDone);
        verify(assignmentStore, times(2)).getSubAssignments(sub1.getName());
        verify(assignmentStore, times(1)).deleteAssignments(anyList());
        assertListEquals(assignments, assignmentCapture.getValue());
        validateCapacity(c0, c1, c2, nodes);

        // should not delete already deleted, may free the capacity though.
        doReturn(new ArrayList<>()).when(assignmentStore).getSubAssignments(sub1.getName());
        aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);

        doReturn(List.of(assignments.get(0))).when(assignmentStore).getSubAssignments(sub1.getName());
        doNothing().when(assignmentStore).deleteAssignments(assignmentCapture.capture());
        vFuture = assignmentManager.unAssignShards(assignments, sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(vFuture::isDone);
        verify(assignmentStore, times(4)).getSubAssignments(sub1.getName());
        verify(assignmentStore, times(2)).deleteAssignments(anyList());
        assertListEquals(List.of(assignments.get(0)), assignmentCapture.getAllValues().get(1));
        validateCapacity(c0, c1, c2, nodes);

        // if nothing to delete, nothing should be deleted. no capacity change as well.
        doReturn(List.of()).when(assignmentStore).getSubAssignments(sub1.getName());
        doNothing().when(assignmentStore).deleteAssignments(assignmentCapture.capture());
        vFuture = assignmentManager.unAssignShards(assignments, sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(vFuture::isDone);
        assertTrue(assignmentCapture.getAllValues().get(2).isEmpty());
        verify(assignmentStore, times(3)).deleteAssignments(anyList());
        validateCapacity(c0, c1, c2, nodes);

        //unAssign should be no-op when there is nothing to unAssign.
        doReturn(assignments).when(assignmentStore).getSubAssignments(sub1.getName());
        doNothing().when(assignmentStore).deleteAssignments(assignmentCapture.capture());
        vFuture = assignmentManager.unAssignShards(new ArrayList<>(), sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(vFuture::isDone);
        assertTrue(assignmentCapture.getAllValues().get(2).isEmpty());
        verify(assignmentStore, times(4)).deleteAssignments(anyList());
        validateCapacity(c0, c1, c2, nodes);
    }


    @Test
    public void testUnAssignShards_Throws() throws Exception {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(3);
        nodes.forEach(this::addConsumerNode);
        List<String> idsToExclude = List.of(nodes.get(1).getConsumerId());

        CompletableFuture<List<Assignment>> aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        List<Assignment> assignments = aFuture.get();
        NodeCapacity c0 = nodes.get(0).getAvailable().clone();
        NodeCapacity c1 = nodes.get(1).getAvailable().clone();
        NodeCapacity c2 = nodes.get(2).getAvailable().clone();

        doThrow(new MetaStoreException("failed to get assignments.")).when(assignmentStore)
                .getSubAssignments(sub1.getName());
        doNothing().when(assignmentStore).deleteAssignments(anyList());

        //validate capacity is not freed in case of exception and delete is not called
        //if getSubAssignment throws.
        CompletableFuture<Void> vFuture = assignmentManager.unAssignShards(assignments, sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(vFuture::isDone);
        assertException(vFuture, MetaStoreException.class, "failed to get assignments.");
        verify(assignmentStore, never()).deleteAssignments(anyList());
        validateCapacity(c0, c1, c2, nodes);

        //validate capacity is not freed in case delete throws
        doReturn(assignments).when(assignmentStore).getSubAssignments(sub1.getName());
        doThrow(new MetaStoreException("failed to delete assignments.")).when(assignmentStore)
                .deleteAssignments(anyList());
        vFuture = assignmentManager.unAssignShards(assignments, sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(vFuture::isDone);
        validateCapacity(c0, c1, c2, nodes);
    }

    @Test
    public void testReAssign() throws Exception {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(3);
        nodes.forEach(this::addConsumerNode);
        List<String> idsToExclude = List.of(nodes.get(1).getConsumerId());

        CompletableFuture<List<Assignment>> aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        List<Assignment> assignments = aFuture.get();

        CompletableFuture<Assignment> rFuture = assignmentManager.reAssignShard(assignments.get(0), sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(rFuture::isDone);
        Assignment ra = rFuture.get();
        assertEquals(assignments.get(0).getShardId(), ra.getShardId());
        assertEquals(nodes.get(1).getConsumerId(), ra.getConsumerId());
    }


    @Test
    public void testReAssign_Throws() throws Exception {
        VaradhiSubscription sub1 =
                SubProvider.getBuilder().setNumShards(2).build("project1.sub1", "project1", "project1.topic1");
        List<SubscriptionUnitShard> shards = SubProvider.shardsOf(sub1);
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(3);
        nodes.forEach(this::addConsumerNode);
        NodeCapacity c0 = nodes.get(0).getAvailable().clone();
        NodeCapacity c1 = nodes.get(1).getAvailable().clone();
        NodeCapacity c2 = nodes.get(2).getAvailable().clone();
        List<String> idsToExclude = List.of(nodes.get(1).getConsumerId());
        CompletableFuture<List<Assignment>> aFuture = assignmentManager.assignShards(shards, sub1, idsToExclude);
        await().atMost(100, TimeUnit.SECONDS).until(aFuture::isDone);
        List<Assignment> assignments = aFuture.get();
        c0.allocate(shards.get(0).getCapacityRequest());
        c2.allocate(shards.get(0).getCapacityRequest());

        doThrow(new MetaStoreException("failed to get subscription assignments.")).when(assignmentStore)
                .getSubAssignments(sub1.getName());
        CompletableFuture<Assignment> rFuture = assignmentManager.reAssignShard(assignments.get(0), sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(rFuture::isDone);
        assertException(rFuture, MetaStoreException.class, "failed to get subscription assignments.");
        verify(assignmentStore, never()).deleteAssignments(anyList());
        validateCapacity(c0, c1, c2, nodes);

        doReturn(assignments, new ArrayList<>()).when(assignmentStore).getSubAssignments(sub1.getName());
        doThrow(new MetaStoreException("failed to create assignments.")).when(assignmentStore)
                .createAssignments(anyList());
        rFuture = assignmentManager.reAssignShard(assignments.get(0), sub1, true);
        await().atMost(100, TimeUnit.SECONDS).until(rFuture::isDone);
        assertException(rFuture, MetaStoreException.class, "failed to create assignments.");
        c0.free(shards.get(0).getCapacityRequest()); //unAssign happened, assigned failed.
        validateCapacity(c0, c1, c2, nodes);
    }


    @Test
    public void testGetAssignments() {
        Assignment a1 = new Assignment("project1.sub1", 0, "node1");
        Assignment a2 = new Assignment("project1.sub2", 0, "node2");
        Assignment a3 = new Assignment("project1.sub1", 0, "node3");
        Assignment a4 = new Assignment("project1.sub3", 0, "node4");
        List<Assignment> allAssignments = Arrays.asList(a1, a2, a3, a4);
        List<Assignment> nodeAssignments = List.of(a3);
        List<Assignment> subAssignments = Arrays.asList(a1, a3);
        doReturn(allAssignments).when(assignmentStore).getAllAssignments();
        doReturn(nodeAssignments).when(assignmentStore).getConsumerNodeAssignments("node3");
        doReturn(subAssignments).when(assignmentStore).getSubAssignments("project1.sub1");
        assertListEquals(allAssignments, assignmentManager.getAllAssignments());
        assertListEquals(nodeAssignments, assignmentManager.getConsumerNodeAssignments("node3"));
        assertListEquals(subAssignments, assignmentManager.getSubAssignments("project1.sub1"));
    }

    @Test
    public void testConsumerNodeJoined() {
        ConsumerNode consumerNode1 = NodeProvider.getConsumerNode("newNode1", NodeProvider.getNodeCapacity(100, 100));
        ConsumerNode consumerNode2 = NodeProvider.getConsumerNode("newNode2", NodeProvider.getNodeCapacity(100, 100));
        ConsumerNode consumerNode3 = NodeProvider.getConsumerNode("newNode3", NodeProvider.getNodeCapacity(100, 100));
        addConsumerNode(consumerNode1);
        addConsumerNode(consumerNode2);
        addConsumerNode(consumerNode1);
        assertFalse(assignmentManager.addConsumerNode(consumerNode1));
        assertTrue(assignmentManager.addConsumerNode(consumerNode3));
    }

    @Test
    public void testConsumerNodeLeft() {
        List<ConsumerNode> nodes = NodeProvider.getConsumerNodes(2);
        addConsumerNode(nodes.get(0));
        addConsumerNode(nodes.get(1));
        removeConsumerNode(nodes.get(0).getConsumerId());
        removeConsumerNode(nodes.get(0).getConsumerId());
        removeConsumerNode(nodes.get(1).getConsumerId());
        removeConsumerNode("some random id");
    }

    private void removeConsumerNode(String consumerId) {
        CompletableFuture<Void> nFuture = assignmentManager.consumerNodeLeft(consumerId);
        await().atMost(1, TimeUnit.SECONDS).until(nFuture::isDone);
        assertFalse(nFuture.isCompletedExceptionally());
    }

    private void addConsumerNode(ConsumerNode node) {
        CompletableFuture<Void> nFuture = assignmentManager.consumerNodeJoined(node);
        await().atMost(1, TimeUnit.SECONDS).until(nFuture::isDone);
        assertFalse(nFuture.isCompletedExceptionally());
    }
    private static void validateCapacity(NodeCapacity c0, NodeCapacity c1, NodeCapacity c2, List<ConsumerNode> nodes) {
        assertEquals(c0, nodes.get(0).getAvailable());
        assertEquals(c1, nodes.get(1).getAvailable());
        assertEquals(c2, nodes.get(2).getAvailable());
    }
}
