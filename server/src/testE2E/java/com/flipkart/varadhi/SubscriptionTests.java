package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SubscriptionTests extends E2EBase {

    private static final Endpoint endpoint =
            new Endpoint.HttpEndpoint(URI.create("http://localhost:8080"), "GET", "", 500, 500, false);
    private static Org o1;
    private static Team o1t1;
    private static Project o1t1p1;
    private static TopicResource p1t1;
    private static TopicResource p1t2;

    private final RetryPolicy retryPolicy = new RetryPolicy(
            new CodeRange[]{new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1, 1, 1, 1
    );
    private final ConsumptionPolicy consumptionPolicy = new ConsumptionPolicy(1, 1, false, 1, null);

    @BeforeAll
    public static void setup() {
        o1 = Org.of("public");
        o1t1 = Team.of("team1", o1.getName());
        o1t1p1 = Project.of("default", "", o1t1.getName(), o1t1.getOrg());
        p1t1 = TopicResource.unGrouped("topic1", o1t1p1.getName(), null);
        p1t2 = TopicResource.grouped("topic2", o1t1p1.getName(), null);
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
                consumptionPolicy
        );
        SubscriptionResource r = makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 200);
        assertSubscriptionEquals(sub, r);

        SubscriptionResource got =
                makeGetRequest(getSubscriptionsUri(o1t1p1, subName), SubscriptionResource.class, 200);
        assertSubscriptionEquals(sub, got);

        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), sub, 409, "Subscription(default.sub1) already exists.", true);
        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), 200);
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
                consumptionPolicy
        );
        makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 200);
        SubscriptionResource created =
                makeGetRequest(getSubscriptionsUri(o1t1p1, subName), SubscriptionResource.class, 200);
        SubscriptionResource update = SubscriptionResource.of(
                created.getName(),
                created.getProject(),
                created.getTopic(),
                created.getTopicProject(),
                "desc updated",
                created.isGrouped(),
                created.getEndpoint(),
                created.getRetryPolicy(),
                created.getConsumptionPolicy()
        );
        //create subscription executes update internally.
        update.setVersion(1);

        SubscriptionResource updated = makeUpdateRequest(getSubscriptionsUri(o1t1p1, subName), update, 200);

        assertEquals(update.getName(), updated.getName());
        assertEquals(update.getDescription(), updated.getDescription());
        assertEquals(2, updated.getVersion());
        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), 200);
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
                consumptionPolicy
        );
        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), shortName, 400, "Invalid Subscription name. Check naming constraints.",
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
                consumptionPolicy
        );
        makeCreateRequest(
                getSubscriptionsUri(Project.of("some_proj", "desc", "someteam", "org")), projectNotExist, 404,
                "Project(some_proj) not found.", true
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
                consumptionPolicy
        );
        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), topicNotExist, 404,
                "Topic(default.some_topic) not found.", true
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
                consumptionPolicy
        );
        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), groupedOnUnGroupTopic, 400,
                "Cannot create grouped Subscription as it's Topic(default.topic1) is not grouped", true
        );
    }
}
