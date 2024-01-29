package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SubscriptionTests extends E2EBase {

    private static final Endpoint endpoint;
    private static Org o1;
    private static Team o1t1;
    private static Project o1t1p1;
    private static TopicResource p1t1;
    private static TopicResource p1t2;

    static {
        try {
            endpoint = new Endpoint.HttpEndpoint(new URL("http", "localhost", "hello"), "GET", "", false);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void setup() {
        o1 = new Org("public", 0);
        o1t1 = new Team("team1", 0, o1.getName());
        o1t1p1 = new Project("default", 0, "", o1t1.getName(), o1t1.getOrg());
        p1t1 = new TopicResource("topic1", 0, o1t1p1.getName(), false, null);
        p1t2 = new TopicResource("topic2", 0, o1t1p1.getName(), true, null);
        makeCreateRequest(getOrgsUri(), o1, 200);
        makeCreateRequest(getTeamsUri(o1t1.getOrg()), o1t1, 200);
        makeCreateRequest(getProjectCreateUri(), o1t1p1, 200);
        makeCreateRequest(getTopicsUri(o1t1p1), p1t1, 200);
        makeCreateRequest(getTopicsUri(o1t1p1), p1t2, 200);
    }

    @AfterAll
    public static void tearDown() {
        // cleanup subs
        cleanupTopic(p1t1.getName(), o1t1p1);
        cleanupTopic(p1t2.getName(), o1t1p1);
        cleanupProject(o1t1p1);
        cleanupTeam(o1t1);
        cleanupOrgs(List.of(o1));
    }

    static String getSubscriptionsUri(Project project) {
        return String.join("/", getProjectUri(project), "subscriptions");
    }

    static String getSubscriptionsUri(Project project, String subscriptionName) {
        return String.join("/", getSubscriptionsUri(project), subscriptionName);
    }

    @Test
    void createSubscription() {
        String subName = "sub1";
        SubscriptionResource sub = new SubscriptionResource(
                subName,
                INITIAL_VERSION,
                o1t1p1.getName(),
                p1t1.getName(),
                p1t1.getProject(),
                "desc",
                false,
                endpoint
        );
        SubscriptionResource r = makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 200);
        assertEquals(sub.getName(), r.getName());
        assertEquals(sub.getProject(), r.getProject());
        assertEquals(sub.getTopic(), r.getTopic());
        assertEquals(sub.isGrouped(), r.isGrouped());

        makeGetRequest(getSubscriptionsUri(o1t1p1, subName), SubscriptionResource.class, 200);
        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), sub, 409, "VaradhiSubscription(default.sub1) already exists.", true);
        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), 200);
    }

    @Test
    void updateSubscription() {
        String subName = "sub1";
        SubscriptionResource sub = new SubscriptionResource(
                subName,
                INITIAL_VERSION,
                o1t1p1.getName(),
                p1t1.getName(),
                p1t1.getProject(),
                "desc",
                false,
                endpoint
        );
        makeCreateRequest(getSubscriptionsUri(o1t1p1), sub, 200);

        SubscriptionResource update = new SubscriptionResource(
                subName,
                INITIAL_VERSION,
                o1t1p1.getName(),
                p1t1.getName(),
                p1t1.getProject(),
                "desc updated",
                false,
                endpoint
        );
        SubscriptionResource updated = makeUpdateRequest(getSubscriptionsUri(o1t1p1, subName), update, 200);
        assertEquals(update.getName(), updated.getName());
        assertEquals(update.getDescription(), updated.getProject());
        assertEquals(1, updated.getVersion());
        makeDeleteRequest(getSubscriptionsUri(o1t1p1, subName), 200);
    }

    @Test
    void createSubscriptionWithValidationFailure() {
        SubscriptionResource shortName = new SubscriptionResource(
                "ab",
                INITIAL_VERSION,
                o1t1p1.getName(),
                p1t1.getName(),
                p1t1.getProject(),
                "desc",
                false,
                endpoint
        );
        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), shortName, 400, "Invalid Subscription name. Check naming constraints.",
                true
        );

        SubscriptionResource projectNotExist = new SubscriptionResource(
                "sub12",
                INITIAL_VERSION,
                "some_proj",
                p1t1.getName(),
                p1t1.getProject(),
                "desc",
                false,
                endpoint
        );
        makeCreateRequest(
                getSubscriptionsUri(new Project("some_proj", 0, "desc", "someteam", "org")), projectNotExist, 404,
                "Project(some_proj) not found.", true
        );

        SubscriptionResource topicNotExist = new SubscriptionResource(
                "sub12",
                INITIAL_VERSION,
                o1t1p1.getName(),
                "some_topic",
                p1t1.getProject(),
                "desc",
                false,
                endpoint
        );
        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), topicNotExist, 404,
                "VaradhiTopic(default.some_topic) not found.", true
        );

        SubscriptionResource groupedOnUnGroupTopic = new SubscriptionResource(
                "sub12",
                INITIAL_VERSION,
                o1t1p1.getName(),
                p1t1.getName(),
                p1t1.getProject(),
                "desc",
                true,
                endpoint
        );
        makeCreateRequest(
                getSubscriptionsUri(o1t1p1), groupedOnUnGroupTopic, 400,
                "Cannot create grouped Subscription as it's Topic(default.topic1) is not grouped", true
        );
    }
}
