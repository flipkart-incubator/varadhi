package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.web.entities.TopicResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;

public class TopicTests extends E2EBase {

    private static final String VaradhiBaseUri = "http://localhost:8080";
    private static final String DefaultTenant = "public";
    private static final String DefaultProject = "default";
    private static Org org1;
    private static Org org2;
    private static Team o1Team1;
    private static Team o2Team1;
    private static Project o1t1Project1;
    private static Project o2t1Project1;

    @BeforeAll
    public static void setup() {
        org1 = Org.of("public");
        org2 = Org.of("public_org2");
        o1Team1 = Team.of("team1", org1.getName());
        o2Team1 = Team.of("team1", org2.getName());
        o1t1Project1 = Project.of("default", "", o1Team1.getName(), o1Team1.getOrg());
        o2t1Project1 = Project.of("default_o2t1", "", o2Team1.getName(), o2Team1.getOrg());
        makeCreateRequest(getOrgsUri(), org1, 200);
        makeCreateRequest(getOrgsUri(), org2, 200);
        makeCreateRequest(getTeamsUri(o1Team1.getOrg()), o1Team1, 200);
        makeCreateRequest(getTeamsUri(o2Team1.getOrg()), o2Team1, 200);
        makeCreateRequest(getProjectCreateUri(), o1t1Project1, 200);
        makeCreateRequest(getProjectCreateUri(), o2t1Project1, 200);
    }

    @AfterAll
    public static void tearDown() {
        cleanupOrgs(List.of(org1, org2));
    }

    @Test
    public void createTopic() {
        String topicName = "test-topic-1";
        TopicResource topic = TopicResource.unGrouped(
            topicName,
            o1t1Project1.getName(),
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        TopicResource r = makeCreateRequest(getTopicsUri(o1t1Project1), topic, 200);

        Assertions.assertEquals(topic.getName(), r.getName());
        Assertions.assertEquals(topic.getProject(), r.getProject());
        Assertions.assertEquals(topic.isGrouped(), r.isGrouped());
        Assertions.assertNotNull(r.getCapacity());
        String errorDuplicateTopic = String.format(
            "Topic '%s' already exists.",
            String.join(NAME_SEPARATOR, topic.getProject(), topic.getName())
        );

        makeCreateRequest(getTopicsUri(o1t1Project1), topic, 409, errorDuplicateTopic, true);
        makeGetRequest(getTopicsUri(o1t1Project1, topicName), TopicResource.class, 200);
        makeDeleteRequest(getTopicsUri(o1t1Project1, topicName), ResourceDeletionType.HARD_DELETE.toString(), 200);
    }

    @Test
    public void createTopicWithValidationFailure() {
        String topicName = "ab";
        TopicResource topic = TopicResource.unGrouped(
            topicName,
            o1t1Project1.getName(),
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        String errorValidationTopic = "Invalid Topic name. Check naming constraints.";

        makeCreateRequest(getTopicsUri(o1t1Project1), topic, 400, errorValidationTopic, true);

        List<String> topics = getTopics(makeListRequest(getTopicsUri(o1t1Project1), 200));
        Assertions.assertTrue(topics.isEmpty());
    }

    @Test
    public void createTopicsWithMultiTenancy() {
        String topicName = "test-topic-2";
        TopicResource topic1 = TopicResource.unGrouped(
            topicName,
            o1t1Project1.getName(),
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        TopicResource topic2 = TopicResource.unGrouped(
            topicName,
            o2t1Project1.getName(),
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );

        TopicResource r1 = makeCreateRequest(getTopicsUri(o1t1Project1), topic1, 200);
        TopicResource r2 = makeCreateRequest(getTopicsUri(o2t1Project1), topic2, 200);

        Assertions.assertEquals(topic1.getName(), r1.getName());
        Assertions.assertEquals(topic1.getProject(), r1.getProject());

        Assertions.assertEquals(topic2.getName(), r2.getName());
        Assertions.assertEquals(topic2.getProject(), r2.getProject());

        makeDeleteRequest(
            getTopicsUri(o1t1Project1, topic1.getName()),
            ResourceDeletionType.HARD_DELETE.toString(),
            200
        );
        makeDeleteRequest(
            getTopicsUri(o2t1Project1, topic2.getName()),
            ResourceDeletionType.HARD_DELETE.toString(),
            200
        );
    }

    @Test
    public void softDeleteTopic() {
        String topicName = "test-topic-3";
        TopicResource topic = TopicResource.unGrouped(
            topicName,
            o1t1Project1.getName(),
            null,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
        makeCreateRequest(getTopicsUri(o1t1Project1), topic, 200);

        makeDeleteRequest(getTopicsUri(o1t1Project1, topicName), ResourceDeletionType.SOFT_DELETE.toString(), 200);

        List<String> topics = getTopics(makeListRequest(getTopicsUri(o1t1Project1), 200));
        Assertions.assertFalse(topics.contains(topicName));

        topics = getTopics(makeListRequest(getTopicsUri(o1t1Project1) + "?includeInactive=true", 200));
        Assertions.assertTrue(topics.contains(topicName));

        makePatchRequest(getTopicsUri(o1t1Project1, topicName) + "/restore", 200);

        TopicResource restoredTopic = makeGetRequest(getTopicsUri(o1t1Project1, topicName), TopicResource.class, 200);
        Assertions.assertEquals(topicName, restoredTopic.getName());

        topics = getTopics(makeListRequest(getTopicsUri(o1t1Project1), 200));
        Assertions.assertTrue(topics.contains(topicName));

        makeDeleteRequest(
                getTopicsUri(o1t1Project1, topic.getName()), ResourceDeletionType.HARD_DELETE.toString(), 200);
    }
}
