package com.flipkart.varadhi.web.admin;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.utils.SubscriptionPropertyValidator;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import com.flipkart.varadhi.web.entities.TopicResource;
import com.google.common.collect.ArrayListMultimap;

import java.net.URI;
import java.util.*;

import static org.mockito.Mockito.mock;

public class SubscriptionTestBase extends WebTestBase {

    private final Random r = new Random();
    private static final Endpoint endpoint =
            new Endpoint.HttpEndpoint(URI.create("http://localhost:8080"), "GET", "", 500, 500, false);
    private static final RetryPolicy retryPolicy = new RetryPolicy(
            new CodeRange[]{new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1, 1, 1, 3
    );
    private static final ConsumptionPolicy consumptionPolicy = new ConsumptionPolicy(10, 1, 1, false, 1, null);
    private static final TopicCapacityPolicy capacityPolicy = new TopicCapacityPolicy(1, 10, 1);
    private static final SubscriptionShards shards = new SubscriptionUnitShard(0, capacityPolicy, null, null, null);
    private static final Map<String, String> subscriptionDefaultProperties =
            SubscriptionPropertyValidator.createPropertyDefaultValueProviders(new RestOptions());
    protected final Project project = Project.of("project1", "", "team1", "org1");
    protected final TopicResource topicResource = TopicResource.unGrouped("topic1", "project2", null);
    SubscriptionService subscriptionService;
    ProjectService projectService;
    VaradhiTopicService topicService;
    VaradhiSubscriptionFactory subscriptionFactory;

    protected String getSubscriptionsUrl(Project project) {
        return String.join("/", "/projects", project.getName(), "subscriptions");
    }

    protected String getSubscriptionUrl(String subscriptionName, Project project) {
        return String.join("/", getSubscriptionsUrl(project), subscriptionName);
    }

    public void setUp()  throws InterruptedException {
        super.setUp();
        subscriptionService = mock(SubscriptionService.class);
        projectService = mock(ProjectService.class);
        topicService = mock(VaradhiTopicService.class);
        subscriptionFactory = mock(VaradhiSubscriptionFactory.class);
        StandardHeaders.initialize(StandardHeaders.fetchDummyHeaderConfiguration());
    }

    public static VaradhiSubscription getUngroupedSubscription(
            String subscriptionName, Project project, VaradhiTopic topic
    ) {
        return getSubscription(subscriptionName, false, project, topic);
    }

    public static VaradhiSubscription getGroupedSubscription(
            String subscriptionName, Project project, VaradhiTopic topic
    ) {
        return getSubscription(subscriptionName, true, project, topic);
    }

    private static VaradhiSubscription getSubscription(
            String subscriptionName, boolean grouped, Project project, VaradhiTopic topic
    ) {
        return VaradhiSubscription.of(
                SubscriptionResource.buildInternalName(project.getName(), subscriptionName),
                project.getName(),
                topic.getName(),
                UUID.randomUUID().toString(),
                grouped,
                endpoint,
                retryPolicy,
                consumptionPolicy,
                shards,
                subscriptionDefaultProperties
        );
    }


    protected SubscriptionResource getSubscriptionResource(
            String subscriptionName, Project project, TopicResource topic
    ) {
        return SubscriptionResource.of(
                subscriptionName,
                project.getName(),
                topic.getName(),
                topic.getProject(),
                "desc",
                false,
                endpoint,
                retryPolicy,
                consumptionPolicy,
                new HashMap<>()
        );
    }

    protected DlqMessage getDlqMessage(int partitionId) {
        ArrayListMultimap<String, String> requestHeaders = ArrayListMultimap.create();
        requestHeaders.put(StandardHeaders.msgIdHeader, Arrays.toString(getRandomBytes(10)));
        requestHeaders.put(StandardHeaders.groupIdHeader, Arrays.toString(getRandomBytes(10)));
        int lId = r.nextInt(30) % 5000;
        int eId = r.nextInt(30) % 40000;
        Offset offset = PulsarOffset.of("mId:%d:%d:%d".formatted(lId, eId, partitionId));
        return new DlqMessage(getRandomBytes(100), requestHeaders, offset,  partitionId);
    }

    private byte[] getRandomBytes(int len) {
        byte[] buf = new byte[len];
        r.nextBytes(buf);
        return buf;
    }

}
