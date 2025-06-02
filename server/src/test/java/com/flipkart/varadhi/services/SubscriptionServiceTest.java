package com.flipkart.varadhi.services;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.core.cluster.ControllerRestApi;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.db.ZKMetaStore;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.entities.cluster.SubscriptionState;
import com.flipkart.varadhi.pulsar.PulsarSubscriptionFactory;
import com.flipkart.varadhi.pulsar.PulsarTopicFactory;
import com.flipkart.varadhi.pulsar.PulsarTopicService;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.entities.PulsarSubscription;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.db.*;
import com.flipkart.varadhi.spi.db.TopicStore;
import com.flipkart.varadhi.spi.services.StorageSubscriptionFactory;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.utils.ShardProvisioner;
import com.flipkart.varadhi.utils.SubscriptionPropertyValidator;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.web.entities.ResourceActionRequest;
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

import static com.flipkart.varadhi.entities.Versioned.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.web.Extensions.ANONYMOUS_IDENTITY;
import static com.flipkart.varadhi.web.admin.SubscriptionTestBase.createGroupedSubscription;
import static com.flipkart.varadhi.web.admin.SubscriptionTestBase.createUngroupedSubscription;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith (VertxExtension.class)
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
    private TopicStore topicStore;
    private SubscriptionStore subscriptionStore;
    private OrgStore orgStore;
    private TeamStore teamStore;
    private ProjectStore projectStore;

    @BeforeEach
    void setUp() throws Exception {
        setupInfrastructure();
        setupTestData();
        setupMocks();
    }

    private void setupInfrastructure() throws Exception {
        JsonMapper.getMapper()
                  .registerSubtypes(
                      new NamedType(PulsarStorageTopic.class, "PulsarTopic"),
                      new NamedType(PulsarSubscription.class, "PulsarSubscription")
                  );

        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(
            CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(),
                new ExponentialBackoffRetry(1000, 1)
            )
        );
        zkCurator.start();

        varadhiMetaStore = spy(new VaradhiMetaStore(new ZKMetaStore(zkCurator)));
        topicStore = spy(varadhiMetaStore.topics());
        subscriptionStore = spy(varadhiMetaStore.subscriptions());
        orgStore = spy(varadhiMetaStore.orgs());
        teamStore = spy(varadhiMetaStore.teams());
        projectStore = spy(varadhiMetaStore.projects());


        orgService = new OrgService(varadhiMetaStore.orgs(), varadhiMetaStore.teams());
        teamService = new TeamService(varadhiMetaStore);
        meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        projectService = new ProjectService(varadhiMetaStore);
    }

    private void setupTestData() {
        org = Org.of("Org");
        team = Team.of("Team", org.getName());
        project1 = Project.of("Project1", "", team.getName(), team.getOrg());
        project2 = Project.of("Project2", "", team.getName(), team.getOrg());
        unGroupedTopic = VaradhiTopic.of(
            "UngroupedTopic",
            project1.getName(),
            false,
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        groupedTopic = VaradhiTopic.of(
            "GroupedTopic",
            project2.getName(),
            true,
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );

        subscription1 = createUngroupedSubscription("Sub1", project1, unGroupedTopic);
        subscription2 = createUngroupedSubscription("Sub2", project1, unGroupedTopic);

        orgService.createOrg(org);
        teamService.createTeam(team);
        projectService.createProject(project1);
        projectService.createProject(project2);
    }

    private void setupMocks() {
        shardProvisioner = mock(ShardProvisioner.class);
        doNothing().when(shardProvisioner).provision(any(), any());

        controllerRestApi = mock(ControllerRestApi.class);
        when(varadhiMetaStore.subscriptions()).thenReturn(subscriptionStore);
        when(varadhiMetaStore.topics()).thenReturn(topicStore);
        when(varadhiMetaStore.projects()).thenReturn(projectStore);
        when(varadhiMetaStore.orgs()).thenReturn(orgStore);
        when(varadhiMetaStore.teams()).thenReturn(teamStore);
        subscriptionService = new SubscriptionService(
            shardProvisioner,
            controllerRestApi,
            subscriptionStore,
            topicStore
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        zkCurator.close();
        zkCuratorTestingServer.close();
    }

    @Test
    void serializeDeserializeSubscription_Success() {
        Endpoint endpoint = new Endpoint.HttpEndpoint(URI.create("http://localhost:8080"), "GET", "", 500, 500, false);

        RetryPolicy retryPolicy = new RetryPolicy(
            new CodeRange[] {new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1,
            1,
            1,
            1
        );

        ConsumptionPolicy consumptionPolicy = new ConsumptionPolicy(10, 1, 1, false, 1, null);

        TopicCapacityPolicy capacity = Constants.DEFAULT_TOPIC_CAPACITY;
        String region = "default";

        TopicPlanner planner = new TopicPlanner(new PulsarConfig());
        StorageSubscriptionFactory subscriptionFactory = new PulsarSubscriptionFactory();
        StorageTopicFactory topicFactory = new PulsarTopicFactory(planner);
        StorageTopicService topicService = new PulsarTopicService(null, planner);

        VaradhiTopic topic = VaradhiTopic.of(
            "GroupedTopic",
            project2.getName(),
            true,
            capacity,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        StorageTopic storageTopic = topicFactory.getTopic(
            topic.getName(),
            project2,
            capacity,
            InternalQueueCategory.MAIN
        );
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
            SubscriptionPropertyValidator.createPropertyDefaultValueProviders(new RestOptions()),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );

        VaradhiSubscriptionFactory varadhiFactory = new VaradhiSubscriptionFactory(
            topicService,
            subscriptionFactory,
            topicFactory,
            region
        );
        VaradhiSubscription subscription = varadhiFactory.get(subscriptionResource, project2, topic);

        String subscriptionJson = JsonMapper.jsonSerialize(subscription);
        VaradhiSubscription deserializedSubscription = JsonMapper.jsonDeserialize(
            subscriptionJson,
            VaradhiSubscription.class
        );

        assertSubscriptionsEqual(subscription, deserializedSubscription);
    }

    @Test
    void serializeDeserializeInternalQueueTypeRetry_Success() {
        InternalQueueType.Retry retryQueue = new InternalQueueType.Retry(1);
        String retryQueueJson = JsonMapper.jsonSerialize(retryQueue);
        InternalQueueType.Retry deserializedRetryQueue = JsonMapper.jsonDeserialize(
            retryQueueJson,
            InternalQueueType.Retry.class
        );

        assertEquals(retryQueue, deserializedRetryQueue);
    }

    @Test
    void serializeDeserializeInternalCompositeSubscription_Success() {
        InternalCompositeSubscription compositeSubscription = subscription1.getShards()
                                                                           .getShard(0)
                                                                           .getDeadLetterSubscription();
        String compositeJson = JsonMapper.jsonSerialize(compositeSubscription);
        InternalCompositeSubscription deserializedComposite = JsonMapper.jsonDeserialize(
            compositeJson,
            InternalCompositeSubscription.class
        );

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
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);
        subscriptionService.createSubscription(unGroupedTopic, subscription2, project1);
        subscriptionService.createSubscription(
            unGroupedTopic,
            createUngroupedSubscription("Sub3", project2, unGroupedTopic),
            project2
        );

        List<String> actualProject1Subscriptions = subscriptionService.getSubscriptionList(project1.getName(), false);
        assertEquals(List.of("Project1.Sub1", "Project1.Sub2"), actualProject1Subscriptions);

        List<String> actualProject2Subscriptions = subscriptionService.getSubscriptionList(project2.getName(), false);
        assertEquals(List.of("Project2.Sub3"), actualProject2Subscriptions);
    }

    @Test
    void getSubscriptionList_MetaStoreFailure_ThrowsException() {
        String projectName = project1.getName();
        when(subscriptionStore.getAllNames(projectName)).thenThrow(new RuntimeException("MetaStore listing failed"));

        Exception exception = assertThrows(
            RuntimeException.class,
            () -> subscriptionService.getSubscriptionList(projectName, false)
        );

        assertEquals("MetaStore listing failed", exception.getMessage());
        verify(subscriptionStore, times(1)).getAllNames(projectName);
    }

    @Test
    void getSubscriptionList_InactiveSubscriptionsAreFilteredOut() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);
        subscriptionService.createSubscription(unGroupedTopic, subscription2, project1);

        subscription2.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Inactive subscription");
        subscriptionStore.update(subscription2);

        when(subscriptionStore.getAllNames(project1.getName())).thenReturn(
            List.of(subscription1.getName(), subscription2.getName())
        );
        when(subscriptionStore.get(subscription1.getName())).thenReturn(subscription1);
        when(subscriptionStore.get(subscription2.getName())).thenReturn(subscription2);

        List<String> actualSubscriptions = subscriptionService.getSubscriptionList(project1.getName(), false);

        assertEquals(List.of(subscription1.getName()), actualSubscriptions);
        verify(subscriptionStore, times(1)).getAllNames(project1.getName());
        verify(subscriptionStore, times(1)).get(subscription1.getName());
        verify(subscriptionStore, times(1)).get(subscription2.getName());
    }

    @Test
    void listSubscriptions_IncludingInactive_ReturnsAllSubscriptions() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);
        subscriptionService.createSubscription(unGroupedTopic, subscription2, project1);

        subscription2.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Inactive subscription");
        subscriptionStore.update(subscription2);

        when(subscriptionStore.getAllNames(project1.getName())).thenReturn(
            List.of(subscription1.getName(), subscription2.getName())
        );

        List<String> actualSubscriptions = subscriptionService.getSubscriptionList(project1.getName(), true);

        assertEquals(List.of(subscription1.getName(), subscription2.getName()), actualSubscriptions);
        verify(subscriptionStore, times(1)).getAllNames(project1.getName());
    }

    @Test
    void getSubscription_ExistingSubscription_ReturnsCorrectSubscription() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
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
    void getSubscription_InactiveSubscription_ReturnsCorrectSubscription() {
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);
        subscription1.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Inactive subscription");
        subscriptionStore.update(subscription1);

        when(subscriptionStore.get(subscription1.getName())).thenReturn(subscription1);

        VaradhiSubscription actualSubscription = subscriptionService.getSubscription(subscription1.getName());

        assertSubscriptionsEqual(subscription1, actualSubscription);
    }

    @Test
    void getSubscription_MetaStoreFailure_ThrowsException() {
        String subscriptionName = subscription1.getName();
        doThrow(new RuntimeException("MetaStore retrieval failed")).when(subscriptionStore).get(subscriptionName);

        Exception exception = assertThrows(
            RuntimeException.class,
            () -> subscriptionService.getSubscription(subscriptionName)
        );

        assertEquals("MetaStore retrieval failed", exception.getMessage());
        verify(subscriptionStore, times(1)).get(subscriptionName);
    }

    @Test
    void createSubscription_ValidUngroupedTopic_CreatesSuccessfully() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());

        VaradhiSubscription result = subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        assertSubscriptionsEqual(subscription1, result);
        assertSubscriptionsEqual(subscription1, subscriptionService.getSubscription(subscription1.getName()));
        verify(shardProvisioner, times(1)).provision(subscription1, project1);
        verify(subscriptionStore, times(1)).create(subscription1);
    }

    @Test
    void createSubscription_NonGroupedTopicWithGroupedSubscription_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        VaradhiSubscription subscription = createGroupedSubscription("Sub1", project1, unGroupedTopic);

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> subscriptionService.createSubscription(unGroupedTopic, subscription, project1)
        );

        String expectedMessage = "Grouped subscription cannot be created or updated for a non-grouped topic '%s'"
                                                                                                                 .formatted(
                                                                                                                     unGroupedTopic.getName()
                                                                                                                 );
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    void createSubscription_GroupedTopic_AllowsBothGroupedAndUngroupedSubscriptions() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        doReturn(groupedTopic).when(topicStore).get(groupedTopic.getName());

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
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        doThrow(new RuntimeException("Provision failed")).when(shardProvisioner).provision(any(), any());

        Exception exception = assertThrows(
            RuntimeException.class,
            () -> subscriptionService.createSubscription(unGroupedTopic, subscription1, project1)
        );

        assertEquals("Provision failed", exception.getMessage());
        assertEquals(LifecycleStatus.State.CREATE_FAILED, subscription1.getStatus().getState());
        verify(subscriptionStore, times(1)).update(subscription1);
    }

    @Test
    void createSubscription_ExistingRetriableSubscription_UpdatesSubscription() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        subscription1.markCreateFailed("Subscription Creation Failed");
        subscriptionStore.update(subscription1);

        doReturn(true).when(subscriptionStore).exists(subscription1.getName());
        doReturn(subscription1).when(subscriptionStore).get(subscription1.getName());

        assertDoesNotThrow(() -> subscriptionService.createSubscription(unGroupedTopic, subscription1, project1));

        verify(shardProvisioner, times(1)).deProvision(subscription1, project1);
        verify(subscriptionStore, times(4)).update(subscription1);
        verify(shardProvisioner, times(2)).provision(subscription1, project1);
    }

    @Test
    void createSubscription_MetaStoreFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        doThrow(new RuntimeException("MetaStore creation failed")).when(subscriptionStore).create(any());

        Exception exception = assertThrows(
            RuntimeException.class,
            () -> subscriptionService.createSubscription(unGroupedTopic, subscription1, project1)
        );

        assertEquals("MetaStore creation failed", exception.getMessage());
        verify(subscriptionStore, times(1)).create(subscription1);
    }

    @Test
    void updateSubscription_ValidInput_UpdatesCorrectly(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);

        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update = createUngroupedSubscription(
            subscription1.getName().split(NAME_SEPARATOR_REGEX)[1],
            project1,
            unGroupedTopic
        );
        update.setVersion(1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
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

        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update = createUngroupedSubscription(
            subscription1.getName().split(NAME_SEPARATOR_REGEX)[1],
            project1,
            unGroupedTopic
        );
        update.setVersion(2);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), REQUESTED_BY);

        String expectedMessage = "Conflicting update detected. Fetch the latest version and try again.";

        InvalidOperationForResourceException exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> updateSubscription(update).join()
        );

        assertEquals(expectedMessage, exception.getMessage());
        checkpoint.flag();
    }

    @Test
    void updateSubscription_UnGroupedTopic_ThrowsException(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);

        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update = createGroupedSubscription(
            subscription1.getName().split(NAME_SEPARATOR_REGEX)[1],
            project1,
            unGroupedTopic
        );
        update.setVersion(1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), REQUESTED_BY);

        String expectedMessage = "Grouped subscription cannot be created or updated for a non-grouped topic '%s'"
                                                                                                                 .formatted(
                                                                                                                     update.getTopic()
                                                                                                                 );
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> updateSubscription(update).join()
        );

        assertEquals(expectedMessage, exception.getMessage());
        checkpoint.flag();
    }

    @Test
    void updateSubscription_MetaStoreFailure_ThrowsRuntimeException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        VaradhiSubscription update = createUngroupedSubscription(
            subscription1.getName().split(NAME_SEPARATOR_REGEX)[1],
            project1,
            unGroupedTopic
        );
        update.setVersion(1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), REQUESTED_BY);

        doThrow(new RuntimeException("MetaStore update failed")).when(subscriptionStore).update(any());

        Exception exception = assertThrows(
            ExecutionException.class,
            () -> subscriptionService.updateSubscription(
                update.getName(),
                update.getVersion(),
                update.getDescription(),
                update.isGrouped(),
                update.getEndpoint(),
                update.getRetryPolicy(),
                update.getConsumptionPolicy(),
                REQUESTED_BY
            ).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("MetaStore update failed", cause.getMessage());

        ArgumentCaptor<VaradhiSubscription> subscriptionCaptor = ArgumentCaptor.forClass(VaradhiSubscription.class);
        verify(subscriptionStore, times(2)).update(subscriptionCaptor.capture());
        VaradhiSubscription capturedSubscription = subscriptionCaptor.getValue();
        assertEquals(update.getName(), capturedSubscription.getName());
        assertEquals(update.getVersion(), capturedSubscription.getVersion());
    }

    @Test
    void deleteSubscription_HardDelete_Success(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);

        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        String subscriptionName = subscription1.getName();
        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(subscriptionName, REQUESTED_BY);

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Delete"
        );

        Future.fromCompletionStage(
            subscriptionService.deleteSubscription(
                subscriptionName,
                project1,
                REQUESTED_BY,
                ResourceDeletionType.HARD_DELETE,
                actionRequest
            )
        ).onComplete(ctx.succeeding(result -> {
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
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState = CompletableFuture.completedFuture(
            SubscriptionState.forStopped()
        );
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Delete"
        );

        assertDoesNotThrow(
            () -> subscriptionService.deleteSubscription(
                subscription1.getName(),
                project1,
                REQUESTED_BY,
                ResourceDeletionType.SOFT_DELETE,
                actionRequest
            ).get()
        );

        verify(subscriptionStore, times(1)).update(subscription1);
        VaradhiSubscription updatedSubscription = subscriptionStore.get(subscription1.getName());
        assertEquals(LifecycleStatus.State.INACTIVE, updatedSubscription.getStatus().getState());
    }

    @Test
    void deleteSubscription_ResourceNotStopped_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> activeState = CompletableFuture.completedFuture(
            SubscriptionState.forRunning()
        );
        doReturn(activeState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Delete"
        );

        ExecutionException exception = assertThrows(
            ExecutionException.class,
            () -> subscriptionService.deleteSubscription(
                subscription1.getName(),
                project1,
                REQUESTED_BY,
                ResourceDeletionType.HARD_DELETE,
                actionRequest
            ).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(IllegalArgumentException.class, cause.getClass());
        assertEquals("Cannot delete subscription in state: SubscriptionState(ASSIGNED, CONSUMING)", cause.getMessage());
        verify(subscriptionStore, never()).delete(subscription1.getName());
    }

    @Test
    void deleteSubscription_MetaStoreFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState = CompletableFuture.completedFuture(
            SubscriptionState.forStopped()
        );
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);
        doThrow(new RuntimeException("MetaStore deletion failed")).when(subscriptionStore)
                                                                  .delete(subscription1.getName());

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Delete"
        );

        ExecutionException exception = assertThrows(
            ExecutionException.class,
            () -> subscriptionService.deleteSubscription(
                subscription1.getName(),
                project1,
                REQUESTED_BY,
                ResourceDeletionType.HARD_DELETE,
                actionRequest
            ).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("MetaStore deletion failed", cause.getMessage());
        verify(subscriptionStore, times(1)).delete(subscription1.getName());
    }

    @Test
    void deleteSubscription_DeProvisionFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState = CompletableFuture.completedFuture(
            SubscriptionState.forStopped()
        );
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);
        doThrow(new RuntimeException("DeProvision failed")).when(shardProvisioner).deProvision(any(), any());

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Delete"
        );

        Exception exception = assertThrows(
            ExecutionException.class,
            () -> subscriptionService.deleteSubscription(
                subscription1.getName(),
                project1,
                REQUESTED_BY,
                ResourceDeletionType.HARD_DELETE,
                actionRequest
            ).get()
        );

        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("DeProvision failed", cause.getMessage());

        VaradhiSubscription updatedSubscription = subscriptionStore.get(subscription1.getName());
        assertEquals(LifecycleStatus.State.DELETE_FAILED, updatedSubscription.getStatus().getState());
        verify(subscriptionStore, times(1)).update(subscription1);
    }

    @Test
    void deleteSubscription_AlreadySoftDeleted_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState = CompletableFuture.completedFuture(
            SubscriptionState.forStopped()
        );
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        ResourceActionRequest softDeleteRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Soft delete"
        );
        subscriptionService.deleteSubscription(
            subscription1.getName(),
            project1,
            REQUESTED_BY,
            ResourceDeletionType.SOFT_DELETE,
            softDeleteRequest
        ).join();

        CompletionException exception = assertThrows(
            CompletionException.class,
            () -> subscriptionService.deleteSubscription(
                subscription1.getName(),
                project1,
                REQUESTED_BY,
                ResourceDeletionType.SOFT_DELETE,
                softDeleteRequest
            ).join()
        );

        assertEquals(IllegalArgumentException.class, exception.getCause().getClass());
        assertEquals("Resource is already in INACTIVE state", exception.getCause().getMessage());
    }

    @Test
    void deleteSubscription_HardDeleteAfterSoftDelete_Success() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        CompletableFuture<SubscriptionState> stoppedState = CompletableFuture.completedFuture(
            SubscriptionState.forStopped()
        );
        doReturn(stoppedState).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        ResourceActionRequest softDeleteRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Soft delete"
        );
        subscriptionService.deleteSubscription(
            subscription1.getName(),
            project1,
            REQUESTED_BY,
            ResourceDeletionType.SOFT_DELETE,
            softDeleteRequest
        ).join();

        ResourceActionRequest hardDeleteRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Hard delete"
        );
        CompletableFuture<Void> result = subscriptionService.deleteSubscription(
            subscription1.getName(),
            project1,
            REQUESTED_BY,
            ResourceDeletionType.HARD_DELETE,
            hardDeleteRequest
        );

        assertDoesNotThrow(result::join);
        verify(subscriptionStore, times(1)).delete(subscription1.getName());
    }

    @Test
    void startSubscription_SuccessfulStart() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
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
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
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
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
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
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
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
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        subscription1.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Inactive subscription");
        subscriptionStore.update(subscription1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Restore"
        );

        CompletableFuture<VaradhiSubscription> result = subscriptionService.restoreSubscription(
            subscription1.getName(),
            REQUESTED_BY,
            actionRequest
        );

        assertDoesNotThrow(() -> {
            VaradhiSubscription restoredSubscription = result.get();
            assertEquals(LifecycleStatus.State.CREATED, restoredSubscription.getStatus().getState());
        });
        verify(subscriptionStore, times(2)).update(subscription1);
    }

    @Test
    void restoreSubscription_UserNotAllowed_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        subscription1.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Inactive subscription");
        subscriptionStore.update(subscription1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.USER_ACTION,
            "Restore"
        );

        InvalidOperationForResourceException exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> subscriptionService.restoreSubscription(subscription1.getName(), REQUESTED_BY, actionRequest).get()
        );

        String expectedMessage = "Restoration denied. Only Varadhi Admin can restore this subscription.";
        assertEquals(expectedMessage, exception.getMessage());
        verify(subscriptionStore, times(2)).update(subscription1);
    }

    @Test
    void restoreSubscription_MetaStoreFailure_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        subscription1.markInactive(LifecycleStatus.ActorCode.SYSTEM_ACTION, "Inactive subscription");
        subscriptionStore.update(subscription1);

        CompletableFuture<SubscriptionState> status = CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(subscription1.getName(), REQUESTED_BY);
        doThrow(new RuntimeException("MetaStore update failed")).when(subscriptionStore).update(any());

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Restore"
        );

        CompletableFuture<VaradhiSubscription> result = subscriptionService.restoreSubscription(
            subscription1.getName(),
            REQUESTED_BY,
            actionRequest
        );

        Exception exception = assertThrows(ExecutionException.class, result::get);
        Throwable cause = exception.getCause();
        assertEquals(RuntimeException.class, cause.getClass());
        assertEquals("MetaStore update failed", cause.getMessage());
        verify(subscriptionStore, times(2)).update(subscription1);
    }

    @Test
    void restoreSubscription_AlreadyWellProvisioned_ThrowsException() {
        doReturn(unGroupedTopic).when(topicStore).get(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, subscription1, project1);

        ResourceActionRequest actionRequest = new ResourceActionRequest(
            LifecycleStatus.ActorCode.SYSTEM_ACTION,
            "Restore"
        );

        InvalidOperationForResourceException exception = assertThrows(
            InvalidOperationForResourceException.class,
            () -> {
                subscriptionService.restoreSubscription(subscription1.getName(), REQUESTED_BY, actionRequest).get();
            }
        );

        String expectedMessage = "Subscription '%s' is already active.".formatted(subscription1.getName());
        assertEquals(expectedMessage, exception.getMessage());
        verify(subscriptionStore, times(1)).update(subscription1);
    }

    private void assertSubscriptionsEqual(VaradhiSubscription expected, VaradhiSubscription actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getProject(), actual.getProject());
        assertEquals(expected.getTopic(), actual.getTopic());
        assertEquals(expected.getDescription(), actual.getDescription());
        assertEquals(expected.isGrouped(), actual.isGrouped());
        assertEquals(expected.getEndpoint().getProtocol(), actual.getEndpoint().getProtocol());
        assertEquals(expected.getRetryPolicy(), actual.getRetryPolicy());
        assertEquals(expected.getConsumptionPolicy(), actual.getConsumptionPolicy());
        assertEquals(expected.getShards().getShardCount(), actual.getShards().getShardCount());
        assertEquals(expected.getStatus().getState(), actual.getStatus().getState());
        assertEquals(expected.getProperties(), actual.getProperties());
    }

    private CompletableFuture<VaradhiSubscription> updateSubscription(VaradhiSubscription to) {
        return subscriptionService.updateSubscription(
            to.getName(),
            to.getVersion(),
            to.getDescription(),
            to.isGrouped(),
            to.getEndpoint(),
            to.getRetryPolicy(),
            to.getConsumptionPolicy(),
            REQUESTED_BY
        );
    }
}
