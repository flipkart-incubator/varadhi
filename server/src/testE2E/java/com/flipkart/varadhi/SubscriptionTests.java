package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.CodeRange;
import com.flipkart.varadhi.entities.ConsumptionPolicy;
import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import com.flipkart.varadhi.web.entities.TopicResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubscriptionTests extends E2EBase {

    private static final Endpoint endpoint = new Endpoint.HttpEndpoint(
        URI.create("http://localhost:8080"),
        "GET",
        "",
        500,
        500,
        false
    );
    private static Org o1;
    private static Team o1t1;
    private static Project o1t1p1;
    private static TopicResource p1t1;
    private static TopicResource p1t2;

    private final RetryPolicy retryPolicy = new RetryPolicy(
        new CodeRange[] {new CodeRange(500, 502)},
        RetryPolicy.BackoffType.LINEAR,
        1,
        1,
        1,
        3
    );
    private final ConsumptionPolicy consumptionPolicy = new ConsumptionPolicy(10, 1, 1, false, 1, null);

    @BeforeAll
    public static void setup() {
        o1 = Org.of("public");
        o1t1 = Team.of("team1", o1.getName());
        o1t1p1 = Project.of("default", "", o1t1.getName(), o1t1.getOrg());
        p1t1 = TopicResource.unGrouped("topic1", o1t1p1.getName(), null, LifecycleStatus.ActorCode.SYSTEM_ACTION);
        p1t2 = TopicResource.grouped("topic2", o1t1p1.getName(), null, LifecycleStatus.ActorCode.SYSTEM_ACTION);
        makeCreateRequest(getOrgsUri(), o1, 200);
        makeCreateRequest(getTeamsUri(o1t1.getOrg()), o1t1, 200);
        makeCreateRequest(getProjectCreateUri(), o1t1p1, 200);
        makeCreateRequest(getTopicsUri(o1t1p1), p1t1, 200);
        makeCreateRequest(getTopicsUri(o1t1p1), p1t2, 200);
    }

    @AfterAll
    public static void tearDown() {
        cleanupOrgs(List.of(o1));
    }

    private static void assertSubscriptionEquals(SubscriptionResource expected, SubscriptionResource actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getProject(), actual.getProject());
        assertEquals(expected.getTopic(), actual.getTopic());
        assertEquals(expected.getTopicProject(), actual.getTopicProject());
        assertEquals(expected.isGrouped(), actual.isGrouped());
    }

    @Test
    void createSubscription() {
        String subName = "sub1";
        SubscriptionResource sub = SubscriptionResource.of(
            subName,
            o1t1p1.getName(),
            p1t1.getName(),
            p1t1.getProject(),
            "desc",
            false,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        SubscriptionResource r = makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 200);
        assertSubscriptionEquals(sub, r);

        SubscriptionResource got = makeGetRequest(
            getSubscriptionsUri(o1t1p1, subName),
            SubscriptionResource.class,
            200
        );
        assertSubscriptionEquals(sub, got);

        makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 409, "Subscription(default.sub1) already exists.", true);
        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), ResourceDeletionType.HARD_DELETE.toString(), 200);
    }

    @Test
    void updateSubscription() {
        String subName = "sub2";
        SubscriptionResource sub = SubscriptionResource.of(
            subName,
            o1t1p1.getName(),
            p1t1.getName(),
            p1t1.getProject(),
            "desc",
            false,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 200);
        SubscriptionResource created = makeGetRequest(
            getSubscriptionsUri(o1t1p1, subName),
            SubscriptionResource.class,
            200
        );
        SubscriptionResource update = SubscriptionResource.of(
            created.getName(),
            created.getProject(),
            created.getTopic(),
            created.getTopicProject(),
            "desc updated",
            created.isGrouped(),
            created.getEndpoint(),
            created.getRetryPolicy(),
            created.getConsumptionPolicy(),
            created.getProperties(),
            created.getActorCode()
        );
        //create subscription executes update internally.
        update.setVersion(1);

        SubscriptionResource updated = makeUpdateRequest(getSubscriptionsUri(o1t1p1, subName), update, 200);

        assertEquals(update.getName(), updated.getName());
        assertEquals(update.getDescription(), updated.getDescription());
        assertEquals(2, updated.getVersion());
        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), ResourceDeletionType.HARD_DELETE.toString(), 200);
    }

    @Test
    void createSubscriptionWithValidationFailure() {
        SubscriptionResource shortName = SubscriptionResource.of(
            "ab",
            o1t1p1.getName(),
            p1t1.getName(),
            p1t1.getProject(),
            "desc",
            false,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        makeCreateRequest(
            getSubscriptionsUri(o1t1p1),
            shortName,
            400,
            "Invalid Subscription name. Check naming constraints.",
            true
        );

        SubscriptionResource projectNotExist = SubscriptionResource.of(
            "sub12",
            "some_proj",
            p1t1.getName(),
            p1t1.getProject(),
            "desc",
            false,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        makeCreateRequest(
            getSubscriptionsUri(Project.of("some_proj", "desc", "someteam", "org")),
            projectNotExist,
            404,
            "Project(some_proj) not found.",
            true
        );

        SubscriptionResource topicNotExist = SubscriptionResource.of(
            "sub12",
            o1t1p1.getName(),
            "some_topic",
            p1t1.getProject(),
            "desc",
            false,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        makeCreateRequest(
            getSubscriptionsUri(o1t1p1),
            topicNotExist,
            404,
            "Topic(default.some_topic) not found.",
            true
        );

        SubscriptionResource groupedOnUnGroupTopic = SubscriptionResource.of(
            "sub12",
            o1t1p1.getName(),
            p1t1.getName(),
            p1t1.getProject(),
            "desc",
            true,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        makeCreateRequest(
            getSubscriptionsUri(o1t1p1),
            groupedOnUnGroupTopic,
            400,
            "Grouped subscription cannot be created or updated for a non-grouped topic 'default.topic1'",
            true
        );
    }

    @Test
    void softDeleteAndRestoreSubscription() {
        String subName = "sub3";
        SubscriptionResource sub = SubscriptionResource.of(
            subName,
            o1t1p1.getName(),
            p1t1.getName(),
            p1t1.getProject(),
            "desc",
            false,
            endpoint,
            retryPolicy,
            consumptionPolicy,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );

        SubscriptionResource createdSub = makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 200);
        assertSubscriptionEquals(sub, createdSub);

        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), ResourceDeletionType.SOFT_DELETE.toString(), 200);

        List<String> subs = getSubscriptions(makeListRequest(getSubscriptionsUri(o1t1p1), 200));
        assertFalse(subs.contains(subName));

        subs = getSubscriptions(makeListRequest(getSubscriptionsUri(o1t1p1) + "?includeInactive=true", 200));
        assertTrue(subs.contains("default." + subName));

        makePatchRequest(getSubscriptionsUri(o1t1p1, subName) + "/restore", 200);

        SubscriptionResource restoredSub = makeGetRequest(
            getSubscriptionsUri(o1t1p1, subName),
            SubscriptionResource.class,
            200
        );
        assertSubscriptionEquals(sub, restoredSub);

        subs = getSubscriptions(makeListRequest(getSubscriptionsUri(o1t1p1), 200));
        assertTrue(subs.contains("default." + subName));

        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), ResourceDeletionType.HARD_DELETE.toString(), 200);
    }
}
