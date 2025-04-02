package com.flipkart.varadhi.services;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.org.OrgMetaStore;
import com.flipkart.varadhi.spi.db.project.ProjectMetaStore;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionMetaStore;
import com.flipkart.varadhi.spi.db.team.TeamMetaStore;
import com.flipkart.varadhi.spi.db.topic.TopicMetaStore;
import com.flipkart.varadhi.web.entities.ResourceActionRequest;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.utils.VaradhiTopicFactory;
import com.flipkart.varadhi.web.entities.TopicResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VaradhiTopicServiceTest {

    private static final String REGION = "local";
    private static final String TOPIC_NAME = "testTopic";
    private static final TopicCapacityPolicy DEFAULT_CAPACITY_POLICY = Constants.DEFAULT_TOPIC_CAPACITY;

    @Mock
    private StorageTopicService<StorageTopic> storageTopicService;

    @Mock
    private MetaStore metaStore;

    @Mock
    private StorageTopicFactory<StorageTopic> storageTopicFactory;

    @Mock
    private VaradhiSubscription subscription;

    @Mock
    private LifecycleStatus status;

    @InjectMocks
    private VaradhiTopicService varadhiTopicService;

    private VaradhiTopicFactory varadhiTopicFactory;
    private Project project;
    private String vTopicName;
    private PulsarStorageTopic pulsarStorageTopic;

    @Mock
    private TopicMetaStore topicMetaStore;
    @Mock
    private SubscriptionMetaStore subscriptionMetaStore;
    @Mock
    private OrgMetaStore orgMetaStore;
    @Mock
    private TeamMetaStore teamMetaStore;
    @Mock
    private ProjectMetaStore projectMetaStore;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        varadhiTopicFactory = spy(new VaradhiTopicFactory(storageTopicFactory, REGION, DEFAULT_CAPACITY_POLICY));
        project = Project.of("default", "", "public", "public");
        vTopicName = String.format("%s.%s", project.getName(), TOPIC_NAME);
        String pTopicName = String.format("persistent://%s/%s/%s", project.getOrg(), project.getName(), vTopicName);

        pulsarStorageTopic = PulsarStorageTopic.of(pTopicName, 1, DEFAULT_CAPACITY_POLICY);
        doReturn(pulsarStorageTopic).when(storageTopicFactory)
                                    .getTopic(vTopicName, project, DEFAULT_CAPACITY_POLICY, InternalQueueCategory.MAIN);

        when(metaStore.topicOperations()).thenReturn(topicMetaStore);
        when(metaStore.subscriptionOperations()).thenReturn(subscriptionMetaStore);
        when(metaStore.orgOperations()).thenReturn(orgMetaStore);
        when(metaStore.projectOperations()).thenReturn(projectMetaStore);
        when(metaStore.teamOperations()).thenReturn(teamMetaStore);
    }

    @Test
    void createVaradhiTopic_SuccessfulCreation() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        varadhiTopicService.create(varadhiTopic, project);

        verify(metaStore.topicOperations(), times(1)).createTopic(varadhiTopic);
        verify(storageTopicService, times(1)).create(pulsarStorageTopic, project);
        verify(storageTopicFactory, times(1)).getTopic(
            vTopicName,
            project,
            DEFAULT_CAPACITY_POLICY,
            InternalQueueCategory.MAIN
        );
    }

    @Test
    void createVaradhiTopic_MetaStoreFailure_ThrowsException() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        doThrow(new VaradhiException("metaStore.topicMetaStore() error")).when(topicMetaStore)
                                                                         .createTopic(varadhiTopic);

        Exception exception = assertThrows(
            VaradhiException.class,
            () -> varadhiTopicService.create(varadhiTopic, project)
        );

        verify(metaStore.topicOperations(), times(1)).createTopic(varadhiTopic);
        assertEquals(VaradhiException.class, exception.getClass());
        assertEquals("metaStore.topicMetaStore() error", exception.getMessage());
    }

    @Test
    void createVaradhiTopic_StorageTopicServiceFailure_ThrowsException() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        doThrow(new VaradhiException("StorageTopicService error")).when(storageTopicService)
                                                                  .create(pulsarStorageTopic, project);

        Exception exception = assertThrows(
            VaradhiException.class,
            () -> varadhiTopicService.create(varadhiTopic, project)
        );

        verify(metaStore.topicOperations(), times(1)).createTopic(varadhiTopic);
        verify(metaStore.topicOperations(), times(1)).updateTopic(varadhiTopic);
        verify(storageTopicService, times(1)).create(pulsarStorageTopic, project);
        assertEquals(VaradhiException.class, exception.getClass());
        assertEquals("StorageTopicService error", exception.getMessage());
        assertEquals(LifecycleStatus.State.CREATE_FAILED, varadhiTopic.getStatus().getState());
    }

    @Test
    void createVaradhiTopic_ExistingRetriableTopic_UpdatesTopic() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        VaradhiTopic existingTopic = spy(varadhiTopic);
        doReturn(true).when(existingTopic).isRetriable();
        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(existingTopic);
        when(metaStore.topicOperations().checkTopicExists(varadhiTopic.getName())).thenReturn(true);

        varadhiTopicService.create(varadhiTopic, project);

        verify(metaStore.topicOperations(), never()).createTopic(varadhiTopic);
        verify(metaStore.topicOperations(), times(2)).updateTopic(varadhiTopic);
        verify(storageTopicService, times(1)).create(pulsarStorageTopic, project);
    }

    @Test
    void deleteVaradhiTopic_SuccessfulHardDelete() {
        VaradhiTopic varadhiTopic = mockDeleteSetup();
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );

        varadhiTopicService.delete(varadhiTopic.getName(), ResourceDeletionType.HARD_DELETE, actionRequest);

        verify(storageTopicService, times(1)).delete(pulsarStorageTopic.getName(), project);
        verify(metaStore.topicOperations(), times(1)).deleteTopic(varadhiTopic.getName());
    }

    @Test
    void deleteVaradhiTopic_StorageTopicDoesNotExist_SuccessfulHardDelete() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );

        when(storageTopicService.exists(pulsarStorageTopic.getName())).thenReturn(false);
        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        when(metaStore.projectOperations().getProject(project.getName())).thenReturn(project);

        varadhiTopicService.delete(varadhiTopic.getName(), ResourceDeletionType.HARD_DELETE, actionRequest);

        verify(storageTopicService, times(1)).delete(pulsarStorageTopic.getName(), project);
        verify(metaStore.topicOperations(), times(1)).deleteTopic(varadhiTopic.getName());
    }

    @Test
    void deleteVaradhiTopic_MetaStoreFailure_ThrowsException() {
        VaradhiTopic varadhiTopic = mockDeleteSetup();
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );

        doThrow(new VaradhiException("metaStore.topicMetaStore() deletion failed")).when(topicMetaStore)
                                                                                   .deleteTopic(varadhiTopic.getName());

        Exception exception = assertThrows(
            VaradhiException.class,
            () -> varadhiTopicService.delete(varadhiTopic.getName(), ResourceDeletionType.HARD_DELETE, actionRequest)
        );

        verify(storageTopicService, times(1)).delete(pulsarStorageTopic.getName(), project);
        verify(metaStore.topicOperations(), times(1)).deleteTopic(varadhiTopic.getName());
        assertEquals(VaradhiException.class, exception.getClass());
        assertEquals("metaStore.topicMetaStore() deletion failed", exception.getMessage());
    }

    @Test
    void deleteVaradhiTopic_TopicInUse_ThrowsException() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();

        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        when(metaStore.subscriptionOperations().getAllSubscriptionNames()).thenReturn(List.of("subscription1"));
        when(metaStore.subscriptionOperations().getSubscription("subscription1")).thenReturn(subscription);
        when(subscription.getTopic()).thenReturn(varadhiTopic.getName());
        when(subscription.getStatus()).thenReturn(status);
        when(status.getState()).thenReturn(LifecycleStatus.State.CREATED);

        Exception exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> varadhiTopicService.delete(varadhiTopic.getName(), ResourceDeletionType.HARD_DELETE, null)
        );

        verify(metaStore.topicOperations(), never()).deleteTopic(varadhiTopic.getName());
        assertEquals(InvalidOperationForResourceException.class, exception.getClass());
        assertEquals("Cannot delete topic as it has existing subscriptions.", exception.getMessage());
    }

    @Test
    void deleteVaradhiTopic_NonExistentTopic_ThrowsException() {
        String nonExistentTopicName = "nonExistentTopic";
        when(metaStore.topicOperations().getTopic(nonExistentTopicName)).thenThrow(
            new ResourceNotFoundException("Topic not found")
        );

        Exception exception = assertThrows(
            ResourceNotFoundException.class,
            () -> varadhiTopicService.delete(nonExistentTopicName, ResourceDeletionType.HARD_DELETE, null)
        );

        assertEquals(ResourceNotFoundException.class, exception.getClass());
        assertEquals("Topic not found", exception.getMessage());
    }

    @Test
    void softDeleteVaradhiTopic_MetaStoreSuccess_UpdatesTopicStatus() {
        VaradhiTopic varadhiTopic = mockDeleteSetup();
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );

        varadhiTopicService.delete(varadhiTopic.getName(), ResourceDeletionType.SOFT_DELETE, actionRequest);

        verify(metaStore.topicOperations(), times(1)).updateTopic(varadhiTopic);
        assertFalse(varadhiTopic.isActive());
    }

    @Test
    void softDeleteVaradhiTopic_MetaStoreFailure_ThrowsException() {
        VaradhiTopic varadhiTopic = mockDeleteSetup();
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );
        doThrow(new VaradhiException("metaStore.topicMetaStore() update failed")).when(topicMetaStore)
                                                                                 .updateTopic(varadhiTopic);

        Exception exception = assertThrows(
            VaradhiException.class,
            () -> varadhiTopicService.delete(varadhiTopic.getName(), ResourceDeletionType.SOFT_DELETE, actionRequest)
        );

        verify(metaStore.topicOperations(), times(1)).updateTopic(varadhiTopic);
        assertEquals(VaradhiException.class, exception.getClass());
        assertEquals("metaStore.topicMetaStore() update failed", exception.getMessage());
    }

    @Test
    void validateTopicForDeletion_SoftDeleteWithInactiveSubscriptions_Success() throws Exception {
        String topicName = "testTopic";

        when(metaStore.subscriptionOperations().getAllSubscriptionNames()).thenReturn(List.of("subscription1"));
        when(metaStore.subscriptionOperations().getSubscription("subscription1")).thenReturn(subscription);
        when(subscription.getTopic()).thenReturn(topicName);
        when(subscription.getStatus()).thenReturn(status);
        when(status.getState()).thenReturn(LifecycleStatus.State.INACTIVE);

        Method method = VaradhiTopicService.class.getDeclaredMethod(
            "validateTopicForDeletion",
            String.class,
            ResourceDeletionType.class
        );
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(varadhiTopicService, topicName, ResourceDeletionType.SOFT_DELETE));
    }

    @Test
    void validateTopicForDeletion_SoftDeleteWithActiveSubscriptions_ThrowsException() throws Exception {
        String topicName = "testTopic";

        when(metaStore.subscriptionOperations().getAllSubscriptionNames()).thenReturn(List.of("subscription1"));
        when(metaStore.subscriptionOperations().getSubscription("subscription1")).thenReturn(subscription);
        when(subscription.getTopic()).thenReturn(topicName);
        when(subscription.getStatus()).thenReturn(status);
        when(status.getState()).thenReturn(LifecycleStatus.State.CREATED);

        Method method = VaradhiTopicService.class.getDeclaredMethod(
            "validateTopicForDeletion",
            String.class,
            ResourceDeletionType.class
        );
        method.setAccessible(true);

        Exception exception = assertThrows(
            InvocationTargetException.class,
            () -> method.invoke(varadhiTopicService, topicName, ResourceDeletionType.SOFT_DELETE)
        );
        assertInstanceOf(InvalidOperationForResourceException.class, exception.getCause());
        assertEquals("Cannot delete topic as it has active subscriptions.", exception.getCause().getMessage());
    }

    @Test
    void validateTopicForDeletion_HardDeleteWithExistingSubscriptions_ThrowsException() throws Exception {
        String topicName = "testTopic";

        when(metaStore.subscriptionOperations().getAllSubscriptionNames()).thenReturn(List.of("subscription1"));
        when(metaStore.subscriptionOperations().getSubscription("subscription1")).thenReturn(subscription);
        when(subscription.getTopic()).thenReturn(topicName);
        when(subscription.getStatus()).thenReturn(status);
        when(status.getState()).thenReturn(LifecycleStatus.State.CREATED);

        Method method = VaradhiTopicService.class.getDeclaredMethod(
            "validateTopicForDeletion",
            String.class,
            ResourceDeletionType.class
        );
        method.setAccessible(true);

        Exception exception = assertThrows(
            InvocationTargetException.class,
            () -> method.invoke(varadhiTopicService, topicName, ResourceDeletionType.HARD_DELETE)
        );
        assertInstanceOf(InvalidOperationForResourceException.class, exception.getCause());
        assertEquals("Cannot delete topic as it has existing subscriptions.", exception.getCause().getMessage());
    }

    @Test
    void validateTopicForDeletion_DeleteWithNoSubscriptions_Success() throws Exception {
        when(metaStore.subscriptionOperations().getAllSubscriptionNames()).thenReturn(List.of());

        Method method = VaradhiTopicService.class.getDeclaredMethod(
            "validateTopicForDeletion",
            String.class,
            ResourceDeletionType.class
        );
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(varadhiTopicService, TOPIC_NAME, ResourceDeletionType.HARD_DELETE));
        assertDoesNotThrow(() -> method.invoke(varadhiTopicService, TOPIC_NAME, ResourceDeletionType.SOFT_DELETE));
    }

    @Test
    void restoreVaradhiTopic_SuccessfulRestore() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        varadhiTopic.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "message");
        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );

        varadhiTopicService.restore(varadhiTopic.getName(), actionRequest);

        verify(metaStore.topicOperations(), times(1)).updateTopic(varadhiTopic);
        Assertions.assertTrue(varadhiTopic.isActive());
    }

    @Test
    void restoreVaradhiTopic_AlreadyActive_ThrowsException() {
        VaradhiTopic varadhiTopic = spy(createVaradhiTopicMock());
        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        doReturn(true).when(varadhiTopic).isActive();
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );

        Exception exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> varadhiTopicService.restore(varadhiTopic.getName(), actionRequest)
        );

        verify(metaStore.topicOperations(), never()).updateTopic(varadhiTopic);
        assertEquals(InvalidOperationForResourceException.class, exception.getClass());
        assertEquals("Topic default.testTopic is not deleted.", exception.getMessage());
    }

    @Test
    void restoreVaradhiTopic_InvalidUser_ThrowsException() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        varadhiTopic.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "message");
        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.USER_ACTION,
            "message"
        );

        Exception exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> varadhiTopicService.restore(varadhiTopic.getName(), actionRequest)
        );

        verify(metaStore.topicOperations(), never()).updateTopic(varadhiTopic);
        assertEquals(InvalidOperationForResourceException.class, exception.getClass());
        assertEquals("Restoration denied. Only Varadhi Admin can restore this topic.", exception.getMessage());
    }

    @Test
    void restoreVaradhiTopic_NonExistentTopic_ThrowsException() {
        String nonExistentTopicName = "nonExistentTopic";
        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "message"
        );

        when(metaStore.topicOperations().getTopic(nonExistentTopicName)).thenThrow(
            new ResourceNotFoundException("Topic not found")
        );

        Exception exception = assertThrows(
            ResourceNotFoundException.class,
            () -> varadhiTopicService.restore(nonExistentTopicName, actionRequest)
        );

        assertEquals(ResourceNotFoundException.class, exception.getClass());
        assertEquals("Topic not found", exception.getMessage());
    }

    @Test
    void checkVaradhiTopicExists_TopicExists_ReturnsTrue() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        when(metaStore.topicOperations().checkTopicExists(varadhiTopic.getName())).thenReturn(true);

        boolean exists = varadhiTopicService.exists(varadhiTopic.getName());

        assertTrue(exists);
        verify(metaStore.topicOperations(), times(1)).checkTopicExists(varadhiTopic.getName());
    }

    @Test
    void checkVaradhiTopicExists_TopicDoesNotExist_ReturnsFalse() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        when(metaStore.topicOperations().checkTopicExists(varadhiTopic.getName())).thenReturn(false);

        boolean exists = varadhiTopicService.exists(varadhiTopic.getName());

        assertFalse(exists);
        verify(metaStore.topicOperations(), times(1)).checkTopicExists(varadhiTopic.getName());
    }

    @Test
    void getVaradhiTopic_TopicExists_ReturnsTopic() {
        VaradhiTopic varadhiTopic = spy(createVaradhiTopicMock());
        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        doReturn(true).when(varadhiTopic).isActive();

        VaradhiTopic retrievedTopic = varadhiTopicService.get(varadhiTopic.getName());

        assertNotNull(retrievedTopic);
        assertEquals(varadhiTopic, retrievedTopic);
    }

    @Test
    void getVaradhiTopic_TopicDoesNotExist_ThrowsException() {
        String nonExistentTopicName = "nonExistentTopic";
        when(metaStore.topicOperations().getTopic(nonExistentTopicName)).thenThrow(
            new ResourceNotFoundException("Topic not found")
        );

        Exception exception = assertThrows(
            ResourceNotFoundException.class,
            () -> varadhiTopicService.get(nonExistentTopicName)
        );

        assertEquals(ResourceNotFoundException.class, exception.getClass());
        assertEquals("Topic not found", exception.getMessage());
    }

    @Test
    void getVaradhiTopicsForProject_ValidProject_ReturnsActiveTopics() {
        String projectName = project.getName();
        List<String> topicNames = List.of("topic1", "topic2");
        List<Boolean> topicStatuses = List.of(true, true);

        setupMockTopics(projectName, topicNames, topicStatuses);

        List<String> activeTopics = varadhiTopicService.getVaradhiTopics(projectName, false);

        assertNotNull(activeTopics);
        assertEquals(2, activeTopics.size());
        assertEquals(topicNames, activeTopics);
    }

    @Test
    void getVaradhiTopicsForProject_MultipleStatuses_FiltersInactiveTopics() {
        String projectName = project.getName();
        List<String> topicNames = List.of("topic1", "topic2", "topic3");
        List<Boolean> topicStatuses = List.of(true, false, true);

        setupMockTopics(projectName, topicNames, topicStatuses);

        List<String> activeTopics = varadhiTopicService.getVaradhiTopics(projectName, false);

        assertNotNull(activeTopics);
        assertEquals(2, activeTopics.size());
        assertEquals(List.of("topic1", "topic3"), activeTopics);
    }

    @Test
    void getVaradhiTopicsForProject_IncludingInactive_ReturnsAllTopics() {
        String projectName = project.getName();
        List<String> topicNames = List.of("topic1", "topic2", "topic3");
        List<Boolean> topicStatuses = List.of(true, false, true);

        setupMockTopics(projectName, topicNames, topicStatuses);

        List<String> allTopics = varadhiTopicService.getVaradhiTopics(projectName, true);

        assertNotNull(allTopics);
        assertEquals(3, allTopics.size());
        assertEquals(topicNames, allTopics);
    }

    private VaradhiTopic createVaradhiTopicMock() {
        TopicResource topicResource = TopicResource.grouped(
            TOPIC_NAME,
            project.getName(),
            DEFAULT_CAPACITY_POLICY,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        return varadhiTopicFactory.get(project, topicResource);
    }

    private VaradhiTopic mockDeleteSetup() {
        VaradhiTopic varadhiTopic = createVaradhiTopicMock();
        when(storageTopicService.exists(pulsarStorageTopic.getName())).thenReturn(true);
        when(metaStore.topicOperations().getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        when(metaStore.projectOperations().getProject(project.getName())).thenReturn(project);
        return varadhiTopic;
    }

    private void setupMockTopics(String projectName, List<String> topicNames, List<Boolean> topicStatuses) {
        if (topicNames.size() != topicStatuses.size()) {
            throw new IllegalArgumentException("Topic names and statuses lists must have the same size");
        }
        when(metaStore.topicOperations().getTopicNames(projectName)).thenReturn(topicNames);
        for (int i = 0; i < topicNames.size(); i++) {
            VaradhiTopic topic = mock(VaradhiTopic.class);
            when(metaStore.topicOperations().getTopic(topicNames.get(i))).thenReturn(topic);
            when(topic.isActive()).thenReturn(topicStatuses.get(i));
        }
    }
}
