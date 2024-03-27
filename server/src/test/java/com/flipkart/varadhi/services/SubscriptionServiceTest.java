package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.VaradhiMetaStore;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.web.admin.SubscriptionHandlersTest.getVaradhiSubscription;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class SubscriptionServiceTest {

    TestingServer zkCuratorTestingServer;
    OrgService orgService;
    TeamService teamService;
    ProjectService projectService;
    CuratorFramework zkCurator;
    VaradhiMetaStore varadhiMetaStore;
    SubscriptionService subscriptionService;
    MeterRegistry meterRegistry;
    Org o1;
    Team o1t1;
    Project o1t1p1, o1t1p2;
    VaradhiTopic unGroupedTopic, groupedTopic;
    VaradhiSubscription sub1, sub2;

    @BeforeEach
    void setUp() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCurator = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCurator.start();
        varadhiMetaStore = spy(new VaradhiMetaStore(zkCurator));

        orgService = new OrgService(varadhiMetaStore);
        teamService = new TeamService(varadhiMetaStore);
        meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        projectService = new ProjectService(varadhiMetaStore, "", meterRegistry);

        o1 = new Org("TestOrg1", 0);
        o1t1 = new Team("TestTeam1", 0, o1.getName());
        o1t1p1 = new Project("o1t1p1", 0, "", o1t1.getName(), o1t1.getOrg());
        o1t1p2 = new Project("o1t1p2", 0, "", o1t1.getName(), o1t1.getOrg());
        unGroupedTopic = VaradhiTopic.of(new TopicResource("topic1", 0, o1t1p1.getName(), false, null));
        groupedTopic = VaradhiTopic.of(new TopicResource("topic2", 0, o1t1p2.getName(), true, null));

        sub1 = getVaradhiSubscription("sub1", o1t1p1, unGroupedTopic);
        sub2 = getVaradhiSubscription("sub2", o1t1p1, unGroupedTopic);

        orgService.createOrg(o1);
        teamService.createTeam(o1t1);
        projectService.createProject(o1t1p1);
        projectService.createProject(o1t1p2);

        subscriptionService = new SubscriptionService(varadhiMetaStore, null);
    }

    @Test
    void getSubscriptionListReturnsCorrectSubscriptions() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        // create multiple subs
        subscriptionService.createSubscription(sub1);
        subscriptionService.createSubscription(sub2);
        subscriptionService.createSubscription(getVaradhiSubscription("sub3", o1t1p2, unGroupedTopic));

        List<String> actualSubscriptions = subscriptionService.getSubscriptionList(o1t1p1.getName());

        assertEquals(List.of("o1t1p1.sub2", "o1t1p1.sub1"), actualSubscriptions);

        actualSubscriptions = subscriptionService.getSubscriptionList(o1t1p2.getName());

        assertEquals(List.of("o1t1p2.sub3"), actualSubscriptions);
    }

    @Test
    void getSubscriptionReturnsCorrectSubscription() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(sub1);

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

        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            subscriptionService.getSubscription(subscriptionName);
        });

        String expectedMessage = "Subscription(%s) not found.".formatted(subscriptionName);
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscription() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        VaradhiSubscription result = subscriptionService.createSubscription(sub1);

        assertSubscriptionsSame(sub1, result);
        assertSubscriptionsSame(sub1, subscriptionService.getSubscription(sub1.getName()));
    }

    @Test
    void testCreateSubscriptionWithNonGroupedTopic() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        VaradhiSubscription subscription = getVaradhiSubscription("sub1", true, o1t1p1, unGroupedTopic);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            subscriptionService.createSubscription(subscription);
        });

        String expectedMessage = "Cannot create grouped Subscription as it's Topic(%s) is not grouped".formatted(
                unGroupedTopic.getName());
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscriptionWithGroupedTopic() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        doReturn(groupedTopic).when(varadhiMetaStore).getTopic(groupedTopic.getName());

        VaradhiSubscription unGroupedSub = getVaradhiSubscription("sub1", false, o1t1p1, groupedTopic);
        VaradhiSubscription groupedSub = getVaradhiSubscription("sub2", true, o1t1p1, groupedTopic);

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(unGroupedSub);
        });

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(groupedSub);
        });
    }

    @Test
    void testCreateSubscriptionWithNonExistentProject() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        Project anonProj = new Project("anonProj", 0, "", "anonTeam", "anonOrg");
        VaradhiSubscription subscription = getVaradhiSubscription("sub1", anonProj, unGroupedTopic);

        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            subscriptionService.createSubscription(subscription);
        });

        String expectedMessage = "Project(%s) not found.".formatted(anonProj.getName());
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscriptionWithNonExistentTopic() {
        VaradhiSubscription subscription = getVaradhiSubscription("sub1", o1t1p1, unGroupedTopic);

        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            subscriptionService.createSubscription(subscription);
        });

        String expectedMessage = "Topic(%s) not found.".formatted(unGroupedTopic.getName());
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void updateSubscriptionUpdatesCorrectly() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(sub1);
        VaradhiSubscription update =
                getVaradhiSubscription(sub1.getName().split(NAME_SEPARATOR_REGEX)[1], o1t1p1, unGroupedTopic);

        VaradhiSubscription updatedSubscription = subscriptionService.updateSubscription(update);

        assertEquals(update.getDescription(), updatedSubscription.getDescription());
        assertEquals(1, updatedSubscription.getVersion());
    }

    @Test
    void updateSubscriptionTopicNotAllowed() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(sub1);
        VaradhiSubscription update =
                getVaradhiSubscription(sub1.getName().split(NAME_SEPARATOR_REGEX)[1], o1t1p1, groupedTopic);

        Exception ex = assertThrows(java.lang.IllegalArgumentException.class, () -> {
            subscriptionService.updateSubscription(update);
        });

        assertEquals("Cannot update Topic of Subscription(%s)".formatted(update.getName()), ex.getMessage());
    }

    @Test
    void updateSubscriptionWithVersionConflictThrows() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(sub1);
        VaradhiSubscription update =
                getVaradhiSubscription(sub1.getName().split(NAME_SEPARATOR_REGEX)[1], o1t1p1, unGroupedTopic);
        update.setVersion(2);

        Exception exception = assertThrows(InvalidOperationForResourceException.class, () -> {
            subscriptionService.updateSubscription(update);
        });

        String expectedMessage =
                "Conflicting update, Subscription(%s) has been modified. Fetch latest and try again.".formatted(
                        update.getName());
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void updateSubscriptionWithUnGroupedTopicThrows() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(sub1);
        VaradhiSubscription update =
                getVaradhiSubscription(sub1.getName().split(NAME_SEPARATOR_REGEX)[1], true, o1t1p1, unGroupedTopic);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            subscriptionService.updateSubscription(update);
        });

        String expectedMessage =
                "Cannot update Subscription(%s) to grouped as it's Topic(%s) is not grouped".formatted(
                        update.getName(),
                        update.getTopic()
                );
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void deleteSubscriptionRemovesSubscription() {
        doReturn(unGroupedTopic).when(varadhiMetaStore).getTopic(unGroupedTopic.getName());
        subscriptionService.createSubscription(sub1);

        String name = sub1.getName();

        VaradhiSubscription subscription = subscriptionService.getSubscription(name);
        assertNotNull(subscription);

        subscriptionService.deleteSubscription(name);

        Exception exception = assertThrows(
                ResourceNotFoundException.class,
                () -> subscriptionService.getSubscription(name)
        );

        String expectedMessage = "Subscription(%s) not found.".formatted(name);
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

}
