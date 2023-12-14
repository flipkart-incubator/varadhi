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

public class TopicTests extends E2EBase {

    private static final String VaradhiBaseUri = "http://localhost:8080";
    private static final String DefaultTenant = "public";
    private static final String DefaultProject = "default";
    private static Org org1;
    private static Team o1Team1;
    private static Project o1t1Project1;


    @BeforeAll
    public static void setup() {
        org1 = new Org("public", 0);
        o1Team1 = new Team("team1", 0, org1.getName());
        o1t1Project1 = new Project("default", 0, "", o1Team1.getName(), o1Team1.getOrg());
        makeCreateRequest(getOrgsUri(), org1, 200);
        makeCreateRequest(getTeamsUri(o1Team1.getOrg()), o1Team1, 200);
        makeCreateRequest(getProjectCreateUri(), o1t1Project1, 200);
    }

    @AfterAll
    public static void tearDown() {
        cleanupOrgs(List.of(org1));
    }


    @Test
    public void createTopic() {
        String topicName = "test-topic-1";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, o1t1Project1.getName(), false, null);
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
        makeDeleteRequest(getTopicsUri(o1t1Project1)+"/"+topicName, 200);
    }

    @Test
    public void createTopicWithValidationFailure() {
        String topicName = "ab";
        TopicResource topic =
                new TopicResource(topicName, Constants.INITIAL_VERSION, o1t1Project1.getName(), false, null);
        String errorValidationTopic = "Invalid Topic name. Check naming constraints.";
        makeCreateRequest(getTopicsUri(o1t1Project1), topic, 400, errorValidationTopic, true);
    }
}
