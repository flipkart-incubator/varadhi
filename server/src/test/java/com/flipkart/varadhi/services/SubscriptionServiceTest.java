package com.flipkart.varadhi.services;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.core.cluster.ControllerRestApi;
import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.entities.*;
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
import com.flipkart.varadhi.web.admin.SubscriptionHandlersTest;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_IDENTITY;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.web.admin.SubscriptionHandlersTest.getGroupedSubscription;
import static com.flipkart.varadhi.web.admin.SubscriptionHandlersTest.getUngroupedSubscription;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class SubscriptionServiceTest {

    TestingServer zkCuratorTestingServer;
    OrgService orgService;
    TeamService teamService;
    ProjectService projectService;
    CuratorFramework zkCurator;
    VaradhiMetaStore varadhiMetaStore;
    ControllerRestApi controllerRestApi;
    SubscriptionService subscriptionService;
    ShardProvisioner shardProvisioner;
    MeterRegistry meterRegistry;
    Org o1;
    Team o1t1;
    Project o1t1p1, o1t1p2;
    VaradhiTopic unGroupedTopic, groupedTopic;
    VaradhiSubscription sub1, sub2;
    String requestedBy = ANONYMOUS_IDENTITY;

    @BeforeEach
    void setUp() throws Exception {
        JsonMapper.getMapper().registerSubtypes(new NamedType(PulsarStorageTopic.class, "PulsarTopic"));
        JsonMapper.getMapper().registerSubtypes(new NamedType(PulsarSubscription.class, "PulsarSubscription"));

        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCurator.start();
        varadhiMetaStore = spy(new VaradhiMetaStore(zkCurator));

        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
        meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        projectService = new ProjectService(varadhiMetaStore, "", meterRegistry);

        o1 = Org.of("TestOrg1");
        o1t1 = Team.of("TestTeam1", o1.getName());
        o1t1p1 = Project.of("o1t1p1", "", o1t1.getName(), o1t1.getOrg());
        o1t1p2 = Project.of("o1t1p2", "", o1t1.getName(), o1t1.getOrg());
        unGroupedTopic = VaradhiTopic.of("topic1", o1t1p1.getName(), false,null);
        groupedTopic = VaradhiTopic.of("topic2", o1t1p2.getName(), true,null);

        sub1 = SubscriptionHandlersTest.getUngroupedSubscription("sub1", o1t1p1, unGroupedTopic);
        sub2 = SubscriptionHandlersTest.getUngroupedSubscription("sub2", o1t1p1, unGroupedTopic);

        orgService.createOrg(o1);
        teamService.createTeam(o1t1);
        projectService.createProject(o1t1p1);
        projectService.createProject(o1t1p2);
        shardProvisioner = mock(ShardProvisioner.class);
        doNothing().when(shardProvisioner).provision(any(), any());
        controllerRestApi = mock(ControllerRestApi.class);

        subscriptionService = new SubscriptionService(shardProvisioner, controllerRestApi, varadhiMetaStore);
    }

    @Test
    void testSubscriptionEntitiesSerDe() {
        Endpoint endpoint;
        RetryPolicy retryPolicy = new RetryPolicy(
                new CodeRange[]{new CodeRange(500, 502)},
                RetryPolicy.BackoffType.LINEAR,
                1, 1, 1, 1
        );
        ConsumptionPolicy consumptionPolicy = new ConsumptionPolicy(10, 1, 1, false, 1, null);
        endpoint = new Endpoint.HttpEndpoint(URI.create("http://localhost:8080"), "GET", "", 500, 500, false);
        TopicCapacityPolicy capacity = Constants.DefaultTopicCapacity;

        String region = "default";
        TopicPlanner planner = new TopicPlanner(new PulsarConfig());
        StorageSubscriptionFactory psf = new PulsarSubscriptionFactory();
        StorageTopicFactory ptf = new PulsarTopicFactory(planner);
        StorageTopicService pts = new PulsarTopicService(null, planner);

        VaradhiTopic vt = VaradhiTopic.of("topic2", o1t1p2.getName(), true, capacity);

        StorageTopic storageTopic = ptf.getTopic(vt.getName(), o1t1p2, capacity, InternalQueueCategory.MAIN);
        vt.addInternalTopic(region, InternalCompositeTopic.of(storageTopic));

        SubscriptionResource subRes = SubscriptionResource.of(
                "sub12",
                o1t1p2.getName(),
                "topic2",
                o1t1p2.getName(),
                "desc",
                false,
                endpoint,
                retryPolicy,
                consumptionPolicy,
                SubscriptionPropertyValidator.createPropertyDefaultValueProviders(new RestOptions())
        );

        VaradhiSubscriptionFactory factory = new VaradhiSubscriptionFactory(pts, psf, ptf, region);
        VaradhiSubscription sub = factory.get(subRes, o1t1p2, vt);

        InternalQueueType.Retry r = new InternalQueueType.Retry(1);
        String rjson = JsonMapper.jsonSerialize(r);
        JsonMapper.jsonDeserialize(rjson, InternalQueueType.Retry.class);


        InternalCompositeSubscription ics1 = sub.getShards().getShard(0).getDeadLetterSubscription();
        String ics1Json = JsonMapper.jsonSerialize(ics1);
        JsonMapper.jsonDeserialize(ics1Json, InternalCompositeSubscription.class);

        RetrySubscription rs1 = sub.getShards().getShard(0).getRetrySubscription();
        String rs1Json = JsonMapper.jsonSerialize(rs1);
        JsonMapper.jsonDeserialize(rs1Json, RetrySubscription.class);

        String json = JsonMapper.jsonSerialize(sub);
        JsonMapper.jsonDeserialize(json, VaradhiSubscription.class);
    }

    @Test
    void getSubscriptionListReturnsCorrectSubscriptions() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        // create multiple subs
        subscriptionService.createSubscription(unGroupedTopic, sub1, o1t1p1);
        subscriptionService.createSubscription(unGroupedTopic, sub2, o1t1p1);
        subscriptionService.createSubscription(
                unGroupedTopic, SubscriptionHandlersTest.getUngroupedSubscription("sub3", o1t1p2, unGroupedTopic),
                o1t1p2
        );

        List<String> actualSubscriptions = subscriptionService.getSubscriptionList(o1t1p1.getName());

        assertEquals(List.of("o1t1p1.sub2", "o1t1p1.sub1"), actualSubscriptions);

        actualSubscriptions = subscriptionService.getSubscriptionList(o1t1p2.getName());

        assertEquals(List.of("o1t1p2.sub3"), actualSubscriptions);
    }

    @Test
    void getSubscriptionReturnsCorrectSubscription() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, sub1, o1t1p1);

        VaradhiSubscription actualSubscription = subscriptionService.getSubscription(sub1.getName());

        assertSubscriptionsSame(sub1, actualSubscription);
    }

    private void assertSubscriptionsSame(VaradhiSubscription expected, VaradhiSubscription actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getTopic(), actual.getTopic());
        assertEquals(expected.isGrouped(), actual.isGrouped());
        assertEquals(expected.getDescription(), actual.getDescription());
    }

    @Test
    void getSubscriptionForNonExistentThrows() {
        String subscriptionName = sub1.getName();

        Exception exception = assertThrows(
                ResourceNotFoundException.class,
                () -> subscriptionService.getSubscription(subscriptionName)
        );

        String expectedMessage = "Subscription(%s) not found.".formatted(subscriptionName);
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscription() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        VaradhiSubscription result = subscriptionService.createSubscription(unGroupedTopic, sub1, o1t1p1);
        assertSubscriptionsSame(sub1, result);
        assertSubscriptionsSame(sub1, subscriptionService.getSubscription(sub1.getName()));
        verify(shardProvisioner, times(1)).provision(sub1, o1t1p1);
    }

    @Test
    void testCreateSubscriptionWithNonGroupedTopic() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        VaradhiSubscription subscription = getGroupedSubscription("sub1", o1t1p1, unGroupedTopic);

        Exception exception = assertThrows(
                IllegalArgumentException.class,
                () -> subscriptionService.createSubscription(unGroupedTopic, subscription, o1t1p1)
        );

        String expectedMessage = "Cannot create grouped Subscription as it's Topic(%s) is not grouped".formatted(
                unGroupedTopic.getName());
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscriptionWithGroupedTopic() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        doReturn(groupedTopic).when(varadhiMetaStore).getTopic(groupedTopic.getName());

        VaradhiSubscription unGroupedSub = getUngroupedSubscription("sub1", o1t1p1, groupedTopic);
        VaradhiSubscription groupedSub = getGroupedSubscription("sub2", o1t1p1, groupedTopic);

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(groupedTopic, unGroupedSub, o1t1p1);
        });

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(groupedTopic, groupedSub, o1t1p1);
        });
    }

    @Test
    void updateSubscriptionUpdatesCorrectly(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, sub1, o1t1p1);
        VaradhiSubscription update =
                SubscriptionHandlersTest.getUngroupedSubscription(
                        sub1.getName().split(NAME_SEPARATOR_REGEX)[1], o1t1p1, unGroupedTopic);
        update.setVersion(1);
        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), requestedBy);

        Future.fromCompletionStage(updateSubscription(update)).onComplete(ctx.succeeding(
                sub -> {
                    assertEquals(update.getDescription(), sub.getDescription());
                    assertEquals(2, sub.getVersion());
                    checkpoint.flag();
                }
        ));
    }

    @Test
    void updateSubscriptionWithVersionConflictThrows(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, sub1, o1t1p1);
        VaradhiSubscription update =
                SubscriptionHandlersTest.getUngroupedSubscription(
                        sub1.getName().split(NAME_SEPARATOR_REGEX)[1], o1t1p1, unGroupedTopic);
        update.setVersion(2);
        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), requestedBy);
        String expectedMessage = "Conflicting update, Subscription has been modified. Fetch latest and try again.";

        InvalidOperationForResourceException e = assertThrows(
                InvalidOperationForResourceException.class, () -> {
                    CompletableFuture<VaradhiSubscription> result = updateSubscription(update);
                    Future.fromCompletionStage(result)
                            .onComplete((v) -> ctx.failNow("Update Subscription didn't fail"));
                }
        );
        assertEquals(expectedMessage, e.getMessage());
        checkpoint.flag();
    }


    @Test
    void updateSubscriptionWithUnGroupedTopicThrows(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, sub1, o1t1p1);
        VaradhiSubscription update =
                getGroupedSubscription(sub1.getName().split(NAME_SEPARATOR_REGEX)[1], o1t1p1, unGroupedTopic);
        update.setVersion(1);
        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(update.getName(), requestedBy);

        String expectedMessage =
                "Cannot update Subscription to grouped as it's Topic(%s) is not grouped".formatted(update.getTopic());
        IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class, () -> {
                    CompletableFuture<VaradhiSubscription> result = updateSubscription(update);
                    Future.fromCompletionStage(result)
                            .onComplete((v) -> ctx.failNow("Update Subscription didn't fail"));
                }
        );
        assertEquals(expectedMessage, e.getMessage());
        checkpoint.flag();
    }

    @Test
    void deleteSubscriptionRemovesSubscription(VertxTestContext ctx) {
        Checkpoint checkpoint = ctx.checkpoint(1);
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(unGroupedTopic, sub1, o1t1p1);

        String name = sub1.getName();

        VaradhiSubscription subscription = subscriptionService.getSubscription(name);
        assertNotNull(subscription);

        CompletableFuture<SubscriptionState> status =
                CompletableFuture.completedFuture(SubscriptionState.forStopped());
        doReturn(status).when(controllerRestApi).getSubscriptionState(name, requestedBy);
        Future.fromCompletionStage(subscriptionService.deleteSubscription(name, o1t1p1, requestedBy))
                .onComplete(ctx.succeeding(
                        v -> {
                            Exception exception = assertThrows(
                                    ResourceNotFoundException.class,
                                    () -> subscriptionService.getSubscription(name)
                            );
                            String expectedMessage = "Subscription(%s) not found.".formatted(name);
                            String actualMessage = exception.getMessage();
                            assertEquals(expectedMessage, actualMessage);
                            checkpoint.flag();
                        }
                ));
    }

    private CompletableFuture<VaradhiSubscription> updateSubscription(VaradhiSubscription to) {
        return subscriptionService.updateSubscription(
                to.getName(), to.getVersion(), to.getDescription(), to.isGrouped(), to.getEndpoint(),
                to.getRetryPolicy(), to.getConsumptionPolicy(), requestedBy
        );
    }

}
