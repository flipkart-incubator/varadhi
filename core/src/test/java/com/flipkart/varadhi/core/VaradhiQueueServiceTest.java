package com.flipkart.varadhi.core;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.core.subscription.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.core.topic.VaradhiTopicFactory;
import com.flipkart.varadhi.entities.CodeRange;
import com.flipkart.varadhi.entities.Constants;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.SubscriptionTestUtils;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.web.QueueResource;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.entities.web.QueueResource.getDefaultSubscriptionName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VaradhiQueueServiceTest {

    private static final String ORG = "o1";
    private static final String TEAM = "t1";
    private static final String PROJECT_NAME = "p1";
    private static final String QUEUE_NAME = "q1";
    private static final TopicCapacityPolicy CAPACITY = new TopicCapacityPolicy(10, 100, 2, 2);
    private static final LifecycleStatus.ActionCode ACTION = LifecycleStatus.ActionCode.USER_ACTION;

    private final Project project = Project.of(PROJECT_NAME, "", TEAM, ORG);
    private final String topicKey = VaradhiTopic.fqn(PROJECT_NAME, QUEUE_NAME);
    private final String defaultSubLocal = getDefaultSubscriptionName(QUEUE_NAME);
    private final String subscriptionKey = SubscriptionResource.buildInternalName(PROJECT_NAME, defaultSubLocal);

    @Mock
    private VaradhiTopicFactory topicFactory;
    @Mock
    private VaradhiTopicService topicService;
    @Mock
    private VaradhiSubscriptionService subscriptionService;
    @Mock
    private VaradhiSubscriptionFactory subscriptionFactory;

    private VaradhiQueueService queueService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        queueService = new VaradhiQueueService(topicFactory, topicService, subscriptionService, subscriptionFactory);
    }

    @Test
    void create_whenTopicAndSubscriptionExistAndLinked_returnsIdempotentGet() {
        VaradhiTopic queueTopic = activeQueueTopic();
        VaradhiSubscription sub = activeLinkedSubscription(queueTopic);

        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);
        when(topicService.get(topicKey)).thenReturn(queueTopic);

        VaradhiQueueService.QueueResult result = queueService.create(sampleQueueResource(), project, ACTION);

        assertEquals(queueTopic, result.topic());
        assertEquals(sub, result.subscription());
        verify(topicFactory, never()).getForQueue(any(), any());
        verify(topicService, never()).create(any(), any());
        verify(subscriptionService, never()).createSubscription(any(), any(), any());
    }

    @Test
    void create_whenSubscriptionPresentButTopicMissing_createsTopicThenSubscription() {
        VaradhiTopic newQueueTopic = queueTopic(VaradhiTopic.TopicCategory.QUEUE);
        VaradhiSubscription built = activeLinkedSubscription(newQueueTopic);

        when(topicService.exists(topicKey)).thenReturn(false);
        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(topicFactory.getForQueue(eq(project), any())).thenReturn(newQueueTopic);
        when(topicService.get(topicKey)).thenReturn(newQueueTopic);
        when(subscriptionFactory.get(any(), eq(project), eq(newQueueTopic))).thenReturn(built);
        when(subscriptionService.createSubscription(eq(newQueueTopic), eq(built), eq(project))).thenReturn(built);

        VaradhiQueueService.QueueResult result = queueService.create(sampleQueueResource(), project, ACTION);

        assertEquals(newQueueTopic, result.topic());
        assertEquals(built, result.subscription());
        verify(topicService).create(eq(newQueueTopic), eq(project));
        verify(subscriptionService).createSubscription(newQueueTopic, built, project);
    }

    @Test
    void create_whenDefaultSubscriptionExistsButPointsAtOtherTopic_throwsInvalidOperation() {
        VaradhiTopic queueTopic = activeQueueTopic();
        VaradhiSubscription orphan = mock(VaradhiSubscription.class);
        when(orphan.getTopic()).thenReturn(VaradhiTopic.fqn(PROJECT_NAME, "other"));

        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(orphan);

        InvalidOperationForResourceException ex = assertThrows(
            InvalidOperationForResourceException.class,
            () -> queueService.create(sampleQueueResource(), project, ACTION)
        );
        assertTrue(ex.getMessage().contains(QUEUE_NAME));
    }

    @Test
    void create_whenExistingTopicIsTopiCategory_throwsInvalidOperation() {
        VaradhiTopic plainTopic = VaradhiTopic.of(
            PROJECT_NAME,
            QUEUE_NAME,
            false,
            CAPACITY,
            ACTION,
            null,
            VaradhiTopic.TopicCategory.TOPIC
        );
        plainTopic.markCreated();
        VaradhiTopic requestedQueueTopic = queueTopic(VaradhiTopic.TopicCategory.QUEUE);

        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.exists(subscriptionKey)).thenReturn(false);
        when(topicFactory.getForQueue(eq(project), any())).thenReturn(requestedQueueTopic);
        doThrow(new DuplicateResourceException("exists")).when(topicService).create(any(), any());
        when(topicService.get(topicKey)).thenReturn(plainTopic);

        assertThrows(
            InvalidOperationForResourceException.class,
            () -> queueService.create(sampleQueueResource(), project, ACTION)
        );
        verify(subscriptionService, never()).createSubscription(any(), any(), any());
    }

    @Test
    void create_whenExistingQueueTopicButGroupedMismatch_throwsInvalidOperation() {
        VaradhiTopic existing = VaradhiTopic.of(
            PROJECT_NAME,
            QUEUE_NAME,
            true,
            CAPACITY,
            ACTION,
            null,
            VaradhiTopic.TopicCategory.QUEUE
        );
        existing.markCreated();
        VaradhiTopic requested = VaradhiTopic.of(
            PROJECT_NAME,
            QUEUE_NAME,
            false,
            CAPACITY,
            ACTION,
            null,
            VaradhiTopic.TopicCategory.QUEUE
        );

        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.exists(subscriptionKey)).thenReturn(false);
        when(topicFactory.getForQueue(eq(project), any())).thenReturn(requested);
        doThrow(new DuplicateResourceException("exists")).when(topicService).create(any(), any());
        when(topicService.get(topicKey)).thenReturn(existing);

        assertThrows(
            InvalidOperationForResourceException.class,
            () -> queueService.create(sampleQueueResource(), project, ACTION)
        );
    }

    @Test
    void create_passesCustomRetryPolicyAndTargetClientIdsToSubscriptionFactory() {
        VaradhiTopic newQueueTopic = queueTopic(VaradhiTopic.TopicCategory.QUEUE);
        VaradhiSubscription built = activeLinkedSubscription(newQueueTopic);
        RetryPolicy customRetry = new RetryPolicy(
            new CodeRange[] {new CodeRange(503, 503)},
            RetryPolicy.BackoffType.EXPONENTIAL,
            2,
            2,
            2,
            5
        );
        Map<String, String> clients = Map.of("http://callback.example/push", "client-b");
        QueueResource body = new QueueResource(
            QUEUE_NAME,
            0,
            PROJECT_NAME,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            customRetry,
            Constants.QueueDefaults.CONSUMPTION_POLICY,
            null,
            clients,
            SubscriptionTestUtils.getSubscriptionDefaultProperties()
        );

        when(topicService.exists(topicKey)).thenReturn(false);
        when(subscriptionService.exists(subscriptionKey)).thenReturn(false);
        when(topicFactory.getForQueue(eq(project), any())).thenReturn(newQueueTopic);
        when(topicService.get(topicKey)).thenReturn(newQueueTopic);
        when(subscriptionFactory.get(any(), eq(project), eq(newQueueTopic))).thenReturn(built);
        when(subscriptionService.createSubscription(any(), any(), any())).thenReturn(built);

        queueService.create(body, project, ACTION);

        ArgumentCaptor<SubscriptionResource> subCap = ArgumentCaptor.forClass(SubscriptionResource.class);
        verify(subscriptionFactory).get(subCap.capture(), eq(project), eq(newQueueTopic));
        assertEquals(customRetry, subCap.getValue().getRetryPolicy());
        assertEquals("client-b", subCap.getValue().getTargetClientIds().get("http://callback.example/push"));
    }

    @Test
    void deleteQueue_afterSubscriptionFutureCompletes_deletesTopic() {
        CompletableFuture<Void> subPhase = new CompletableFuture<>();
        VaradhiSubscription sub = activeLinkedSubscription(activeQueueTopic());
        VaradhiTopic topic = activeQueueTopic();

        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);
        when(
            subscriptionService.deleteSubscription(
                eq(subscriptionKey),
                eq(project),
                any(),
                eq(ResourceDeletionType.HARD_DELETE),
                any()
            )
        ).thenReturn(subPhase);
        when(topicService.exists(topicKey)).thenReturn(true);
        when(topicService.get(topicKey)).thenReturn(topic);

        CompletableFuture<Void> all = queueService.deleteQueue(
            PROJECT_NAME,
            QUEUE_NAME,
            project,
            "user",
            ResourceDeletionType.HARD_DELETE,
            new RequestActionType(ACTION, "")
        );

        verify(topicService, never()).delete(any(), any(), any());
        subPhase.complete(null);
        all.join();
        verify(topicService).delete(eq(topicKey), eq(ResourceDeletionType.HARD_DELETE), any());
    }

    @Test
    void deleteQueue_whenSubscriptionAbsent_stillDeletesTopicWhenPresent() {
        VaradhiTopic topic = activeQueueTopic();

        when(subscriptionService.exists(subscriptionKey)).thenReturn(false);
        when(topicService.exists(topicKey)).thenReturn(true);
        when(topicService.get(topicKey)).thenReturn(topic);

        queueService.deleteQueue(
            PROJECT_NAME,
            QUEUE_NAME,
            project,
            "user",
            ResourceDeletionType.HARD_DELETE,
            new RequestActionType(ACTION, "")
        ).join();

        verify(subscriptionService, never()).deleteSubscription(any(), any(), any(), any(), any());
        verify(topicService).delete(eq(topicKey), eq(ResourceDeletionType.HARD_DELETE), any());
    }

    @Test
    void restoreQueue_whenSubscriptionMissing_throwsResourceNotFound() {
        when(subscriptionService.exists(subscriptionKey)).thenReturn(false);

        ResourceNotFoundException ex = assertThrows(
            ResourceNotFoundException.class,
            () -> queueService.restoreQueue(PROJECT_NAME, QUEUE_NAME, "u", new RequestActionType(ACTION, ""))
        );
        assertTrue(ex.getMessage().contains("default subscription"));
    }

    @Test
    void restoreQueue_whenTopicMissing_throwsResourceNotFound() {
        VaradhiSubscription sub = activeLinkedSubscription(activeQueueTopic());
        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(topicService.exists(topicKey)).thenReturn(false);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);

        ResourceNotFoundException ex = assertThrows(
            ResourceNotFoundException.class,
            () -> queueService.restoreQueue(PROJECT_NAME, QUEUE_NAME, "u", new RequestActionType(ACTION, ""))
        );
        assertTrue(ex.getMessage().contains("topic not found"));
    }

    @Test
    void restoreQueue_whenTopicAndSubscriptionActive_skipsRestoreCalls() {
        VaradhiTopic topic = activeQueueTopic();
        VaradhiSubscription sub = activeLinkedSubscription(topic);

        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);
        when(topicService.get(topicKey)).thenReturn(topic);

        queueService.restoreQueue(PROJECT_NAME, QUEUE_NAME, "u", new RequestActionType(ACTION, "")).join();

        verify(subscriptionService, never()).restoreSubscription(any(), any(), any());
        verify(topicService, never()).restore(any(), any());
    }

    @Test
    void restoreQueue_whenSubscriptionInactive_restoresSubscriptionThenTopic() {
        VaradhiTopic topic = queueTopic(VaradhiTopic.TopicCategory.QUEUE);
        topic.markInactive(ACTION, "");
        VaradhiSubscription sub = inactiveLinkedSubscription(topic);

        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);
        when(topicService.get(topicKey)).thenReturn(topic);
        when(subscriptionService.restoreSubscription(eq(subscriptionKey), any(), any())).thenReturn(
            CompletableFuture.completedFuture(sub)
        );

        queueService.restoreQueue(PROJECT_NAME, QUEUE_NAME, "u", new RequestActionType(ACTION, "")).join();

        verify(subscriptionService).restoreSubscription(eq(subscriptionKey), any(), any());
        verify(topicService).restore(eq(topicKey), any());
    }

    @Test
    void updateQueue_delegatesToSubscriptionUpdate_returnsTopicAndSubscription() {
        VaradhiTopic topic = activeQueueTopic();
        VaradhiSubscription sub = activeLinkedSubscription(topic);
        VaradhiSubscription updated = activeLinkedSubscription(topic);
        updated.setVersion(3);

        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);
        when(topicService.get(topicKey)).thenReturn(topic);
        when(
            subscriptionService.updateSubscription(
                eq(subscriptionKey),
                eq(2),
                any(),
                anyBoolean(),
                any(),
                any(),
                any(),
                eq("user")
            )
        ).thenReturn(CompletableFuture.completedFuture(updated));

        QueueResource body = sampleQueueResource();
        body.setCapacity(CAPACITY);
        body.setVersion(2);

        VaradhiQueueService.QueueResult result = queueService.updateQueue(
            PROJECT_NAME,
            QUEUE_NAME,
            body,
            "user",
            ACTION
        ).join();

        verify(topicService).updateTopicState(any(VaradhiTopic.class));
        assertEquals(topic, result.topic());
        assertEquals(updated, result.subscription());
    }

    @Test
    void updateQueue_whenPathNameDoesNotMatchBody_throws() {
        QueueResource body = sampleQueueResource();
        assertThrows(
            IllegalArgumentException.class,
            () -> queueService.updateQueue(PROJECT_NAME, "other", body, "user", ACTION)
        );
    }

    @Test
    void updateQueue_whenSubscriptionMissing_throwsResourceNotFound() {
        when(subscriptionService.exists(subscriptionKey)).thenReturn(false);

        ResourceNotFoundException ex = assertThrows(
            ResourceNotFoundException.class,
            () -> queueService.updateQueue(PROJECT_NAME, QUEUE_NAME, sampleQueueResource(), "user", ACTION)
        );
        assertTrue(ex.getMessage().contains("update"));
        assertTrue(ex.getMessage().contains("default subscription"));
    }

    @Test
    void updateQueue_persistsTopicCapacityFromBody_beforeSubscriptionUpdate() {
        VaradhiTopic topic = activeQueueTopic();
        VaradhiSubscription sub = activeLinkedSubscription(topic);
        VaradhiSubscription updated = activeLinkedSubscription(topic);

        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);
        when(topicService.get(topicKey)).thenReturn(topic);
        when(
            subscriptionService.updateSubscription(
                eq(subscriptionKey),
                anyInt(),
                any(),
                anyBoolean(),
                any(),
                any(),
                any(),
                any()
            )
        ).thenReturn(CompletableFuture.completedFuture(updated));

        QueueResource body = sampleQueueResource();
        body.setCapacity(TopicCapacityPolicy.getDefault());
        body.setVersion(1);

        queueService.updateQueue(PROJECT_NAME, QUEUE_NAME, body, "user", ACTION).join();

        ArgumentCaptor<VaradhiTopic> topicCap = ArgumentCaptor.forClass(VaradhiTopic.class);
        verify(topicService).updateTopicState(topicCap.capture());
        assertEquals(TopicCapacityPolicy.getDefault(), topicCap.getValue().getCapacity());
        assertEquals(VaradhiTopic.TopicCategory.QUEUE, topicCap.getValue().getTopicCategory());
    }

    @Test
    void updateQueue_whenStoredTopicIsNotQueueCategory_throwsInvalidOperation() {
        VaradhiTopic plainTopic = queueTopic(VaradhiTopic.TopicCategory.TOPIC);
        plainTopic.markCreated();
        VaradhiSubscription sub = activeLinkedSubscription(plainTopic);

        when(subscriptionService.exists(subscriptionKey)).thenReturn(true);
        when(topicService.exists(topicKey)).thenReturn(true);
        when(subscriptionService.getSubscription(subscriptionKey)).thenReturn(sub);
        when(topicService.get(topicKey)).thenReturn(plainTopic);

        QueueResource body = sampleQueueResource();
        body.setCapacity(CAPACITY);

        InvalidOperationForResourceException ex = assertThrows(
            InvalidOperationForResourceException.class,
            () -> queueService.updateQueue(PROJECT_NAME, QUEUE_NAME, body, "user", ACTION)
        );
        assertTrue(ex.getMessage().contains("not a queue topic"));
    }

    private VaradhiTopic queueTopic(VaradhiTopic.TopicCategory category) {
        return VaradhiTopic.of(PROJECT_NAME, QUEUE_NAME, false, CAPACITY, ACTION, null, category);
    }

    private VaradhiTopic activeQueueTopic() {
        VaradhiTopic t = queueTopic(VaradhiTopic.TopicCategory.QUEUE);
        t.markCreated();
        return t;
    }

    private VaradhiSubscription activeLinkedSubscription(VaradhiTopic topic) {
        VaradhiSubscription s = SubscriptionTestUtils.createUngroupedSubscription(defaultSubLocal, project, topic);
        s.markCreated();
        return s;
    }

    private VaradhiSubscription inactiveLinkedSubscription(VaradhiTopic topic) {
        VaradhiSubscription s = SubscriptionTestUtils.createUngroupedSubscription(defaultSubLocal, project, topic);
        s.markInactive(ACTION, "");
        return s;
    }

    private QueueResource sampleQueueResource() {
        return new QueueResource(
            QUEUE_NAME,
            0,
            PROJECT_NAME,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            Constants.QueueDefaults.RETRY_POLICY,
            Constants.QueueDefaults.CONSUMPTION_POLICY,
            null,
            Map.of("http://localhost:8080", "c1"),
            SubscriptionTestUtils.getSubscriptionDefaultProperties()
        );
    }
}
