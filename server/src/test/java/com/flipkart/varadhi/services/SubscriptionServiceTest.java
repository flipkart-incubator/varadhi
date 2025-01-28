package com.flipkart.varadhi.services;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.core.cluster.ControllerRestApi;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.entities.CodeRange;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.InternalCompositeSubscription;
import com.flipkart.varadhi.entities.InternalCompositeTopic;
import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.RetrySubscription;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionState;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.pulsar.PulsarSubscriptionFactory;
import com.flipkart.varadhi.pulsar.PulsarTopicFactory;
import com.flipkart.varadhi.pulsar.PulsarTopicService;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.entities.PulsarSubscription;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.StorageSubscriptionFactory;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.utils.JsonMapper;
import com.flipkart.varadhi.utils.ShardProvisioner;
import com.flipkart.varadhi.utils.SubscriptionPropertyValidator;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_IDENTITY;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.web.admin.SubscriptionTestBase.createGroupedSubscription;
import static com.flipkart.varadhi.web.admin.SubscriptionTestBase.createUngroupedSubscription;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class SubscriptionServiceTest {

    private TestingServer zkCuratorTestingServer;
    private CuratorFramework zkCurator;
    private VaradhiMetaStore varadhiMetaStore;
    private OrgService orgService;
    private TeamService teamService;
    private ProjectService projectService;
    private SubscriptionService subscriptionService;
    private ShardProvisioner shardProvisioner;
    private ControllerRestApi controllerRestApi;
    private MeterRegistry meterRegistry;

    private Org org;
    private Team team;
    private Project project1, project2;
    private VaradhiTopic unGroupedTopic, groupedTopic;
    private VaradhiSubscription subscription1, subscription2;

    private static final String REQUESTED_BY = ANONYMOUS_IDENTITY;

    @BeforeEach
    void setUp() throws Exception {
        JsonMapper.getMapper().registerSubtypes(
                new NamedType(PulsarStorageTopic.class, "PulsarTopic"),
                new NamedType(PulsarSubscription.class, "PulsarSubscription")
        );

        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)
        ));
        zkCurator.start();

        varadhiMetaStore = spy(new VaradhiMetaStore(zkCurator));

        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
        meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        projectService = new ProjectService(varadhiMetaStore, "", meterRegistry);

        org = Org.of("Org");
        team = Team.of("Team", org.getName());
        project1 = Project.of("Project1", "", team.getName(), team.getOrg());
        project2 = Project.of("Project2", "", team.getName(), team.getOrg());
        unGroupedTopic = VaradhiTopic.of("UngroupedTopic", project1.getName(), false, null);
        groupedTopic = VaradhiTopic.of("GroupedTopic", project2.getName(), true, null);

        subscription1 = createUngroupedSubscription("Sub1", project1, unGroupedTopic);
        subscription2 = createUngroupedSubscription("Sub2", project1, unGroupedTopic);

        orgService.createOrg(org);
        teamService.createTeam(team);
        projectService.createProject(project1);
        projectService.createProject(project2);

        shardProvisioner = mock(ShardProvisioner.class);
        doNothing().when(shardProvisioner).provision(any(), any());

        controllerRestApi = mock(ControllerRestApi.class);

        subscriptionService = new SubscriptionService(shardProvisioner, controllerRestApi, varadhiMetaStore);
    }

    @AfterEach
    void tearDown() throws Exception {
        zkCurator.close();
        zkCuratorTestingServer.close();
    }

    @Test
    void serializeDeserializeSubscription_Success() {
        Endpoint endpoint = new Endpoint.HttpEndpoint(
                URI.create("http://localhost:8080"), "GET", "",
                500, 500, false
        );

        RetryPolicy retryPolicy = new RetryPolicy(
                new CodeRange[]{new CodeRange(500, 502)},
                RetryPolicy.BackoffType.LINEAR,
                1, 1, 1, 1
        );

        ConsumptionPolicy consumptionPolicy = new ConsumptionPolicy(
                10, 1,
                1, false, 1, null
        );

        TopicCapacityPolicy capacity = Constants.DefaultTopicCapacity;
        String region = "default";

        TopicPlanner planner = new TopicPlanner(new PulsarConfig());
        StorageSubscriptionFactory subscriptionFactory = new PulsarSubscriptionFactory();
        StorageTopicFactory topicFactory = new PulsarTopicFactory(planner);
        StorageTopicService topicService = new PulsarTopicService(null, planner);

        VaradhiTopic topic = VaradhiTopic.of("GroupedTopic", project2.getName(), true, capacity);
        StorageTopic storageTopic =
                topicFactory.getTopic(topic.getName(), project2, capacity, InternalQueueCategory.MAIN);
        topic.addInternalTopic(region, InternalCompositeTopic.of(storageTopic));

        SubscriptionResource subscriptionResource = SubscriptionResource.of(
                "SubscriptionResource",
                project2.getName(),
                "GroupedTopic",
                project2.getName(),
                "Description",
                false,
                endpoint,
                retryPolicy,
                consumptionPolicy,
                SubscriptionPropertyValidator.createPropertyDefaultValueProviders(new RestOptions())
        );

        VaradhiSubscriptionFactory varadhiFactory =
                new VaradhiSubscriptionFactory(topicService, subscriptionFactory, topicFactory, region);
        VaradhiSubscription subscription = varadhiFactory.get(subscriptionResource, project2, topic);

        String subscriptionJson = JsonMapper.jsonSerialize(subscription);
        VaradhiSubscription deserializedSubscription =
                JsonMapper.jsonDeserialize(subscriptionJson, VaradhiSubscription.class);

        assertEquals(subscription.getName(), deserializedSubscription.getName());
        assertEquals(subscription.getProject(), deserializedSubscription.getProject());
        assertEquals(subscription.getTopic(), deserializedSubscription.getTopic());
        assertEquals(subscription.getDescription(), deserializedSubscription.getDescription());
        assertEquals(subscription.isGrouped(), deserializedSubscription.isGrouped());
        assertEquals(subscription.getEndpoint().getProtocol(), deserializedSubscription.getEndpoint().getProtocol());
        assertEquals(subscription.getRetryPolicy(), deserializedSubscription.getRetryPolicy());
        assertEquals(subscription.getConsumptionPolicy(), deserializedSubscription.getConsumptionPolicy());
        assertEquals(subscription.getShards().getShardCount(), deserializedSubscription.getShards().getShardCount());
        assertEquals(subscription.getStatus(), deserializedSubscription.getStatus());
        assertEquals(subscription.getProperties(), deserializedSubscription.getProperties());
    }

    @Test
    void serializeDeserializeInternalQueueTypeRetry_Success() {
        InternalQueueType.Retry retryQueue = new InternalQueueType.Retry(1);
        String retryQueueJson = JsonMapper.jsonSerialize(retryQueue);
        InternalQueueType.Retry deserializedRetryQueue =
                JsonMapper.jsonDeserialize(retryQueueJson, InternalQueueType.Retry.class);

        assertEquals(retryQueue, deserializedRetryQueue);
    }

    @Test
    void serializeDeserializeInternalCompositeSubscription_Success() {
        InternalCompositeSubscription compositeSubscription =
                subscription1.getShards().getShard(0).getDeadLetterSubscription();
        String compositeJson = JsonMapper.jsonSerialize(compositeSubscription);
        InternalCompositeSubscription deserializedComposite =
                JsonMapper.jsonDeserialize(compositeJson, InternalCompositeSubscription.class);

        assertEquals(compositeSubscription, deserializedComposite);
    }

    @Test
    void serializeDeserializeRetrySubscription_Success() {
        RetrySubscription retrySubscription = subscription1.getShards().getShard(0).getRetrySubscription();
        String retryJson = JsonMapper.jsonSerialize(retrySubscription);
        RetrySubscription deserializedRetry = JsonMapper.jsonDeserialize(retryJson, RetrySubscription.class);

        assertEquals(retrySubscription, deserializedRetry);
    }

    @Test
    void getSubscriptionList_ValidProject_ReturnsCorrectSubscriptions() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);
        subscriptionService.createSubscription(unGroupedTopic, subscription2, project1);
        subscriptionService.createSubscription(
                unGroupedTopic, createUngroupedSubscription("Sub3", project2, unGroupedTopic),
                project2
        );

        List<String> actualProject1Subscriptions = subscriptionService.getSubscriptionList(project1.getName());
        assertEquals(List.of("Project1.Sub1", "Project1.Sub2"), actualProject1Subscriptions);

        List<String> actualProject2Subscriptions = subscriptionService.getSubscriptionList(project2.getName());
        assertEquals(List.of("Project2.Sub3"), actualProject2Subscriptions);
    }

    @Test
    void getSubscriptionList_MetaStoreFailure_ThrowsException() {
        String projectName = project1.getName();
        when(varadhiMetaStore.getSubscriptionNames(projectName))
                .thenThrow(new RuntimeException("MetaStore listing failed"));

        Exception exception = assertThrows(
                RuntimeException.class,
                () -> subscriptionService.getSubscriptionList(projectName)
        );

        assertEquals("MetaStore listing failed", exception.getMessage());
        verify(varadhiMetaStore, times(1)).getSubscriptionNames(projectName);
    }

    @Test
    void getSubscriptionList_InactiveSubscriptionsAreFilteredOut() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);
        subscriptionService.createSubscription(unGroupedTopic, subscription2, project1);

        subscription2.markInactive();
        varadhiMetaStore.updateSubscription(subscription2);

        when(varadhiMetaStore.getSubscriptionNames(project1.getName()))
                .thenReturn(List.of(subscription1.getName(), subscription2.getName()));
        when(varadhiMetaStore.getSubscription(subscription1.getName())).thenReturn(subscription1);
        when(varadhiMetaStore.getSubscription(subscription2.getName())).thenReturn(subscription2);

        List<String> actualSubscriptions = subscriptionService.getSubscriptionList(project1.getName());

        assertEquals(List.of(subscription1.getName()), actualSubscriptions);
        verify(varadhiMetaStore, times(1)).getSubscriptionNames(project1.getName());
        verify(varadhiMetaStore, times(1)).getSubscription(subscription1.getName());
        verify(varadhiMetaStore, times(1)).getSubscription(subscription2.getName());
    }

    @Test
    void getSubscription_ExistingSubscription_ReturnsCorrectSubscription() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription actualSubscription = subscriptionService.getSubscription(subscription1.getName());

        assertSubscriptionsEqual(subscription1, actualSubscription);
    }

    @Test
    void getSubscription_NonExistentSubscription_ThrowsException() {
        String subscriptionName = subscription1.getName();

        Exception exception = assertThrows(
                ResourceNotFoundException.class,
                () -> subscriptionService.getSubscription(subscriptionName)
        );

        assertEquals("Subscription(%s) not found.".formatted(subscriptionName), exception.getMessage());
    }

    @Test
    void getSubscription_InactiveSubscription_ThrowsException() {
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);
        subscription1.markInactive();
        varadhiMetaStore.updateSubscription(subscription1);

        when(varadhiMetaStore.getSubscription(subscription1.getName())).thenReturn(subscription1);

        Exception exception = assertThrows(
                ResourceNotFoundException.class,
                () -> subscriptionService.getSubscription(subscription1.getName())
        );

        assertEquals(
                "Subscription '%s' not found or in invalid state.".formatted(subscription1.getName()),
                exception.getMessage()
        );
        verify(varadhiMetaStore, times(1)).getSubscription(subscription1.getName());
    }

    @Test
    void getSubscription_MetaStoreFailure_ThrowsException() {
        String subscriptionName = subscription1.getName();
        doThrow(new RuntimeException("MetaStore retrieval failed")).when(varadhiMetaStore)
                .getSubscription(subscriptionName);

        Exception exception = assertThrows(
                RuntimeException.class,
                () -> subscriptionService.getSubscription(subscriptionName)
        );

        assertEquals("MetaStore retrieval failed", exception.getMessage());
        verify(varadhiMetaStore, times(1)).getSubscription(subscriptionName);
    }

    @Test
    void createSubscription_ValidUngroupedTopic_CreatesSuccessfully() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());

        VaradhiSubscription result = subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        assertSubscriptionsEqual(subscription1, result);
        assertSubscriptionsEqual(subscription1, subscriptionService.getSubscription(subscription1.getName()));
        verify(shardProvisioner, times(1)).provision(subscription1, project1);
        verify(varadhiMetaStore, times(1)).createSubscription(subscription1);
    }

    @Test
    void createSubscription_NonGroupedTopicWithGroupedSubscription_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        VaradhiSubscription subscription = createGroupedSubscription("Sub1", project1, unGroupedTopic);

        Exception exception = assertThrows(
                IllegalArgumentException.class,
                () -> subscriptionService.createSubscription(unGroupedTopic, subscription, project1)
        );

        String expectedMessage =
                "Grouped subscription cannot be created or updated for a non-grouped topic '%s'".formatted(
                unGroupedTopic.getName());
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    void createSubscription_GroupedTopic_AllowsBothGroupedAndUngroupedSubscriptions() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        doReturn(groupedTopic).when(varadhiMetaStore).getTopic(groupedTopic.getName());

        VaradhiSubscription unGroupedSub = createUngroupedSubscription("Sub1", project1, groupedTopic);
        VaradhiSubscription groupedSub = createGroupedSubscription("Sub2", project1, groupedTopic);

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(groupedTopic, unGroupedSub, project1);
        });

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(groupedTopic, groupedSub, project1);
        });
    }

    @Test
    void createSubscription_ProvisionFailure_SetsStateToCreateFailed() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        doThrow(new RuntimeException("Provision failed")).when(shardProvisioner).provision(any(), any());

        Exception exception = assertThrows(
                RuntimeException.class,
                () -> subscriptionService.createSubscription(unGroupedTopic, subscription1, project1)
        );

        assertEquals("Provision failed", exception.getMessage());
        assertEquals(VaradhiSubscription.State.CREATE_FAILED, subscription1.getStatus().getState());
        verify(varadhiMetaStore, times(1)).updateSubscription(subscription1);
    }

    @Test
    void createSubscription_MetaStoreFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        doThrow(new RuntimeException("MetaStore creation failed")).when(varadhiMetaStore).createSubscription(any());

        Exception exception = assertThrows(
                RuntimeException.class,
                () -> subscriptionService.createSubscription(unGroupedTopic, subscription1, project1)
        );

        assertEquals("MetaStore creation failed", exception.getMessage());
        verify(varadhiMetaStore, times(1)).createSubscription(subscription1);
    }

    @Test
    void updateSubscription_ValidInput_UpdatesCorrectly(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);

        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update =
                createUngroupedSubscription(
                        subscription1.getName().split(NAME_SEPARATOR_REGEX)[1], project1, unGroupedTopic);
        update.setVersion(1);

        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), REQUESTED_BY);

        Future.fromCompletionStage(updateSubscription(update)).onComplete(ctx.succeeding(sub -> {
            assertEquals(update.getDescription(), sub.getDescription());
            assertEquals(2, sub.getVersion());
            checkpoint.flag();
        }));
    }

    @Test
    void updateSubscription_VersionConflict_ThrowsException(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);

        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update =
                createUngroupedSubscription(
                        subscription1.getName().split(NAME_SEPARATOR_REGEX)[1], project1, unGroupedTopic);
        update.setVersion(2);

        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), REQUESTED_BY);

        String expectedMessage = "Conflicting update detected. Fetch the latest version and try again.";

        InvalidOperationForResourceException exception = assertThrows(
                InvalidOperationForResourceException.class, () -> updateSubscription(update).join()
        );

        assertEquals(expectedMessage, exception.getMessage());
        checkpoint.flag();
    }

    @Test
    void updateSubscription_UnGroupedTopic_ThrowsException(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);

        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update =
                createGroupedSubscription(
                        subscription1.getName().split(NAME_SEPARATOR_REGEX)[1], project1, unGroupedTopic);
        update.setVersion(1);

        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), REQUESTED_BY);

        String expectedMessage =
                "Grouped subscription cannot be created or updated for a non-grouped topic '%s'".formatted(
                        update.getTopic());
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> updateSubscription(update).join()
        );

        assertEquals(expectedMessage, exception.getMessage());
        checkpoint.flag();
    }

    @Test
    void updateSubscription_MetaStoreFailure_ThrowsRuntimeException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update = createUngroupedSubscription(
                subscription1.getName().split(NAME_SEPARATOR_REGEX)[1], project1, unGroupedTopic);
        update.setVersion(1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), REQUESTED_BY);

        doThrow(new RuntimeException("MetaStore update failed")).when(varadhiMetaStore).updateSubscription(any());

        Exception exception = assertThrows(
                ExecutionException.class,
                () -> subscriptionService.updateSubscription(
                        update.getName(), update.getVersion(), update.getDescription(), update.isGrouped(),
                        update.getEndpoint(), update.getRetryPolicy(), update.getConsumptionPolicy(), REQUESTED_BY
                ).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("MetaStore update failed", cause.getMessage());

        ArgumentCaptor<VaradhiSubscription> subscriptionCaptor = ArgumentCaptor.forClass(VaradhiSubscription.class);
        verify(varadhiMetaStore, times(2)).updateSubscription(subscriptionCaptor.capture());
        VaradhiSubscription capturedSubscription = subscriptionCaptor.getValue();
        assertEquals(update.getName(), capturedSubscription.getName());
        assertEquals(update.getVersion(), capturedSubscription.getVersion());
    }

    @Test
    void deleteSubscription_HardDelete_Success(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);

        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        String subscriptionName = subscription1.getName();
        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(subscriptionName, REQUESTED_BY);

        Future.fromCompletionStage(
                        subscriptionService.deleteSubscription(
                                subscriptionName, project1, REQUESTED_BY, ResourceDeletionType.HARD_DELETE))
                .onComplete(ctx.succeeding(result -> {
                    ResourceNotFoundException exception = assertThrows(
                            ResourceNotFoundException.class,
                            () -> subscriptionService.getSubscription(subscriptionName)
                    );
                    assertEquals("Subscription(%s) not found.".formatted(subscriptionName), exception.getMessage());
                    checkpoint.flag();
                }));
    }

    @Test
    void deleteSubscription_SoftDelete_UpdatesSubscriptionState() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        assertDoesNotThrow(() -> subscriptionService.deleteSubscription(
                subscription1.getName(), project1, REQUESTED_BY,
                ResourceDeletionType.SOFT_DELETE
        ).get());

        verify(varadhiMetaStore, times(1)).updateSubscription(subscription1);
        VaradhiSubscription updatedSubscription = varadhiMetaStore.getSubscription(subscription1.getName());
        assertEquals(VaradhiSubscription.State.INACTIVE, updatedSubscription.getStatus().getState());
    }

    @Test
    void deleteSubscription_ResourceNotStopped_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> activeState =
                CompletableFuture.completedFuture(SubscriptionState.forRunning());
        doReturn(activeState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> subscriptionService.deleteSubscription(
                        subscription1.getName(), project1, REQUESTED_BY, ResourceDeletionType.HARD_DELETE).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(IllegalArgumentException.class, cause.getClass());
        assertEquals("Cannot delete subscription in state: SubscriptionState(ASSIGNED, CONSUMING)", cause.getMessage());
        verify(varadhiMetaStore, never()).deleteSubscription(subscription1.getName());
    }

    @Test
    void deleteSubscription_MetaStoreFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);
        doThrow(new RuntimeException("MetaStore deletion failed")).when(varadhiMetaStore)
                .deleteSubscription(subscription1.getName());

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> subscriptionService.deleteSubscription(
                        subscription1.getName(), project1, REQUESTED_BY, ResourceDeletionType.HARD_DELETE).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("MetaStore deletion failed", cause.getMessage());
        verify(varadhiMetaStore, times(1)).deleteSubscription(subscription1.getName());
    }

    @Test
    void deleteSubscription_DeProvisionFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);
        doThrow(new RuntimeException("DeProvision failed")).when(shardProvisioner).deProvision(any(), any());

        Exception exception = assertThrows(
                ExecutionException.class,
                () -> subscriptionService.deleteSubscription(
                        subscription1.getName(), project1, REQUESTED_BY, ResourceDeletionType.HARD_DELETE).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("DeProvision failed", cause.getMessage());

        VaradhiSubscription updatedSubscription = varadhiMetaStore.getSubscription(subscription1.getName());
        assertEquals(VaradhiSubscription.State.DELETE_FAILED, updatedSubscription.getStatus().getState());
        verify(varadhiMetaStore, times(1)).updateSubscription(subscription1);
    }

    @Test
    void startSubscription_SuccessfulStart() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionOperation> startFuture = CompletableFuture.completedFuture(
                SubscriptionOperation.startOp(subscription1.getName(), REQUESTED_BY)
        );
        doReturn(startFuture).when(controllerRestApi).startSubscription(subscription1.getName(), REQUESTED_BY);

        assertDoesNotThrow(() -> subscriptionService.start(subscription1.getName(), REQUESTED_BY).get());
        verify(controllerRestApi, times(1)).startSubscription(subscription1.getName(), REQUESTED_BY);
    }

    @Test
    void stopSubscription_SuccessfulStop() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionOperation> stopFuture = CompletableFuture.completedFuture(
                SubscriptionOperation.stopOp(subscription1.getName(), REQUESTED_BY)
        );
        doReturn(stopFuture).when(controllerRestApi).stopSubscription(subscription1.getName(), REQUESTED_BY);

        assertDoesNotThrow(() -> subscriptionService.stop(subscription1.getName(), REQUESTED_BY).get());
        verify(controllerRestApi, times(1)).stopSubscription(subscription1.getName(), REQUESTED_BY);
    }

    @Test
    void startSubscription_NotProvisioned_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        doThrow(new RuntimeException("Provision failed")).when(shardProvisioner).provision(any(), any());

        assertThrows(
                RuntimeException.class,
                () -> subscriptionService.createSubscription(unGroupedTopic, subscription1, project1)
        );

        InvalidOperationForResourceException exception = assertThrows(
                InvalidOperationForResourceException.class,
                () -> subscriptionService.start(subscription1.getName(), REQUESTED_BY).get()
        );

        String expectedMessage = "Subscription 'Project1.Sub1' is not well-provisioned for this operation.";
        assertEquals(expectedMessage, exception.getMessage());
        verify(controllerRestApi, times(0)).startSubscription(subscription1.getName(), REQUESTED_BY);
    }

    @Test
    void stopSubscription_NotProvisioned_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        doThrow(new RuntimeException("Provision failed")).when(shardProvisioner).provision(any(), any());

        assertThrows(
                RuntimeException.class,
                () -> subscriptionService.createSubscription(unGroupedTopic, subscription1, project1)
        );

        InvalidOperationForResourceException exception = assertThrows(
                InvalidOperationForResourceException.class,
                () -> subscriptionService.stop(subscription1.getName(), REQUESTED_BY).get()
        );

        String expectedMessage = "Subscription 'Project1.Sub1' is not well-provisioned for this operation.";
        assertEquals(expectedMessage, exception.getMessage());
        verify(controllerRestApi, times(0)).stopSubscription(subscription1.getName(), REQUESTED_BY);
    }

    @Test
    void restoreSubscription_Success() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        subscription1.markInactive();
        varadhiMetaStore.updateSubscription(subscription1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        CompletableFuture<VaradhiSubscription> result =
                subscriptionService.restoreSubscription(subscription1.getName(), REQUESTED_BY);

        assertDoesNotThrow(() -> {
            VaradhiSubscription restoredSubscription = result.get();
            assertEquals(VaradhiSubscription.State.CREATED, restoredSubscription.getStatus().getState());
        });
        verify(varadhiMetaStore, times(2)).updateSubscription(subscription1);
    }

    @Test
    void restoreSubscription_MetaStoreFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        subscription1.markInactive();
        varadhiMetaStore.updateSubscription(subscription1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);
        doThrow(new RuntimeException("MetaStore update failed")).when(varadhiMetaStore).updateSubscription(any());

        CompletableFuture<VaradhiSubscription> result =
                subscriptionService.restoreSubscription(subscription1.getName(), REQUESTED_BY);

        Exception exception = assertThrows(ExecutionException.class, result::get);
        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("MetaStore update failed", cause.getMessage());
        verify(varadhiMetaStore, times(2)).updateSubscription(subscription1);
    }

    @Test
    void restoreSubscription_AlreadyWellProvisioned_ThrowsException() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        InvalidOperationForResourceException exception = assertThrows(
                InvalidOperationForResourceException.class, () -> {
                    subscriptionService.restoreSubscription(subscription1.getName(), REQUESTED_BY).get();
                }
        );

        String expectedMessage = "Subscription '%s' is already active.".formatted(subscription1.getName());
        assertEquals(expectedMessage, exception.getMessage());
        verify(varadhiMetaStore, times(1)).updateSubscription(subscription1);
    }

    private void assertSubscriptionsEqual(VaradhiSubscription expected, VaradhiSubscription actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getTopic(), actual.getTopic());
        assertEquals(expected.isGrouped(), actual.isGrouped());
        assertEquals(expected.getDescription(), actual.getDescription());
    }

    private CompletableFuture<VaradhiSubscription> updateSubscription(VaradhiSubscription to) {
        return subscriptionService.updateSubscription(
                to.getName(), to.getVersion(), to.getDescription(), to.isGrouped(), to.getEndpoint(),
                to.getRetryPolicy(), to.getConsumptionPolicy(), REQUESTED_BY
        );
    }
}
