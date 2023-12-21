package com.flipkart.varadhi;

import com.flipkart.varadhi.db.ZNode;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.TopicResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.flipkart.varadhi.entities.VaradhiResource.INITIAL_VERSION;

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
        org1 = new Org("public", 0);
        org2 = new Org("public_org2", 0);
        o1Team1 = new Team("team1", 0, org1.getName());
        o2Team1 = new Team("team1", 0, org2.getName());
        o1t1Project1 = new Project("default", 0, "", o1Team1.getName(), o1Team1.getOrg());
        o2t1Project1 = new Project("default_o2t1", 0, "", o2Team1.getName(), o2Team1.getOrg());
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
        TopicResource topic =
                new TopicResource(topicName, INITIAL_VERSION, o1t1Project1.getName(), false, null);
        TopicResource r = makeCreateRequest(getTopicsUri(o1t1Project1), topic, 200);
        Assertions.assertEquals(topic.getVersion(), r.getVersion());
        Assertions.assertEquals(topic.getName(), r.getName());
        Assertions.assertEquals(topic.getProject(), r.getProject());
        Assertions.assertEquals(topic.isGrouped(), r.isGrouped());
        Assertions.assertNull(r.getCapacityPolicy());
        //TODO::fix this.
        String errorDuplicateTopic =
                String.format(
                        "Specified Topic(%s) already exists.",
                        ZNode.getResourceFQDN(topic.getProject(), topic.getName())
                );
        makeCreateRequest(getTopicsUri(o1t1Project1), topic, 409, errorDuplicateTopic, true);
        makeGetRequest(getTopicsUri(o1t1Project1)+"/"+topicName, TopicResource.class, 200);
        makeDeleteRequest(getTopicsUri(o1t1Project1)+"/"+topicName, 200);
    }

    @Test
    public void createTopicWithValidationFailure() {
        String topicName = "ab";
        TopicResource topic =
                new TopicResource(topicName, INITIAL_VERSION, o1t1Project1.getName(), false, null);
        String errorValidationTopic = "Invalid Topic name. Check naming constraints.";
        makeCreateRequest(getTopicsUri(o1t1Project1), topic, 400, errorValidationTopic, true);

        List<String> topics = getTopics(makeListRequest(getTopicsUri(o1t1Project1),200));
        Assertions.assertTrue(topics.isEmpty());
    }

    @Test
    public void createTopicsWithMultiTenancy() {
        String topicName = "test-topic-1";
        TopicResource topic1 =
                new TopicResource(topicName, INITIAL_VERSION, o1t1Project1.getName(), false, null);
        TopicResource topic2 =
                new TopicResource(topicName, INITIAL_VERSION, o2t1Project1.getName(), false, null);

        TopicResource r1 = makeCreateRequest(getTopicsUri(o1t1Project1), topic1, 200);
        TopicResource r2 = makeCreateRequest(getTopicsUri(o2t1Project1), topic2, 200);

        Assertions.assertEquals(topic1.getVersion(), r1.getVersion());
        Assertions.assertEquals(topic1.getName(), r1.getName());
        Assertions.assertEquals(topic1.getProject(), r1.getProject());

        Assertions.assertEquals(topic2.getVersion(), r2.getVersion());
        Assertions.assertEquals(topic2.getName(), r2.getName());
        Assertions.assertEquals(topic2.getProject(), r2.getProject());
    }
}
