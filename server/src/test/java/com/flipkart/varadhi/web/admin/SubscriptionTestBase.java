package com.flipkart.varadhi.web.admin;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.*;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class SubscriptionTestBase extends WebTestBase {

    private final Random random = new Random();

    private static final Endpoint DEFAULT_ENDPOINT = new Endpoint.HttpEndpoint(
        URI.create("http://localhost:8080"),
        "GET",
        "",
        500,
        500,
        false
    );

    private static final RetryPolicy DEFAULT_RETRY_POLICY = new RetryPolicy(
        new CodeRange[] {new CodeRange(500, 502)},
        RetryPolicy.BackoffType.LINEAR,
        1,
        1,
        1,
        3
    );

    private static final ConsumptionPolicy DEFAULT_CONSUMPTION_POLICY = new ConsumptionPolicy(10, 1, 1, false, 1, null);

    private static final TopicCapacityPolicy DEFAULT_CAPACITY_POLICY = new TopicCapacityPolicy(1, 10, 1);

    private static final SubscriptionShards DEFAULT_SHARDS = new SubscriptionUnitShard(
        0,
        DEFAULT_CAPACITY_POLICY,
        null,
        null,
        null
    );

    private static final Map<String, String> DEFAULT_SUBSCRIPTION_PROPERTIES = SubscriptionPropertyValidator
                                                                                                            .createPropertyDefaultValueProviders(
                                                                                                                new RestOptions()
                                                                                                            );

    protected static final Project PROJECT = Project.of("project1", "", "team1", "org1");
    protected static final TopicResource TOPIC_RESOURCE = TopicResource.unGrouped(
        "topic1",
        "project1",
        null,
        LifecycleStatus.ActorCode.SYSTEM_ACTION,
        "test"
    );

    @Mock
    protected SubscriptionService subscriptionService;

    @Mock
    protected ProjectService projectService;

    @Mock
    protected VaradhiTopicService topicService;

    @Mock
    protected VaradhiSubscriptionFactory subscriptionFactory;

    @Override
    public void setUp() throws InterruptedException {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    protected String buildSubscriptionsUrl(Project project) {
        return String.format("/projects/%s/subscriptions", project.getName());
    }

    protected String buildSubscriptionUrl(String subscriptionName, Project project) {
        return String.format("%s/%s", buildSubscriptionsUrl(project), subscriptionName);
    }

    public static VaradhiSubscription createUngroupedSubscription(
        String subscriptionName,
        Project project,
        VaradhiTopic topic
    ) {
        return createSubscription(subscriptionName, false, project, topic);
    }

    public static VaradhiSubscription createGroupedSubscription(
        String subscriptionName,
        Project project,
        VaradhiTopic topic
    ) {
        return createSubscription(subscriptionName, true, project, topic);
    }

    private static VaradhiSubscription createSubscription(
        String subscriptionName,
        boolean grouped,
        Project project,
        VaradhiTopic topic
    ) {
        return VaradhiSubscription.of(
            SubscriptionResource.buildInternalName(project.getName(), subscriptionName),
            project.getName(),
            topic.getName(),
            UUID.randomUUID().toString(),
            grouped,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            DEFAULT_SUBSCRIPTION_PROPERTIES,
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
    }

    protected SubscriptionResource createSubscriptionResource(
        String subscriptionName,
        Project project,
        TopicResource topic
    ) {
        return createSubscriptionResource(subscriptionName, project, topic, DEFAULT_RETRY_POLICY);
    }

    protected SubscriptionResource createSubscriptionResource(
        String subscriptionName,
        Project project,
        TopicResource topic,
        RetryPolicy retryPolicy
    ) {
        return SubscriptionResource.of(
            subscriptionName,
            project.getName(),
            topic.getName(),
            topic.getProject(),
            "Description",
            false,
            DEFAULT_ENDPOINT,
            retryPolicy,
            DEFAULT_CONSUMPTION_POLICY,
            new HashMap<>(),
            LifecycleStatus.ActorCode.SYSTEM_ACTION
        );
    }

    protected DlqMessage createDlqMessage(int partitionId) {
        var requestHeaders = ArrayListMultimap.<String, String>create();
        requestHeaders.put(StdHeaders.get().msgId(), generateRandomHex(10));
        requestHeaders.put(StdHeaders.get().groupId(), generateRandomHex(10));

        int lId = random.nextInt(5000);
        int eId = random.nextInt(40000);
        Offset offset = PulsarOffset.of("mId:%d:%d:%d".formatted(lId, eId, partitionId));

        return new DlqMessage(generateRandomBytes(100), requestHeaders, offset, partitionId);
    }

    private String generateRandomHex(int length) {
        byte[] bytes = generateRandomBytes(length);
        StringBuilder hexBuilder = new StringBuilder();
        for (byte b : bytes) {
            hexBuilder.append(String.format("%02x", b));
        }
        return hexBuilder.toString();
    }

    private byte[] generateRandomBytes(int length) {
        byte[] buffer = new byte[length];
        random.nextBytes(buffer);
        return buffer;
    }

    protected RetryPolicy createCustomRetryPolicy(int retryAttempts) {
        return new RetryPolicy(
            new CodeRange[] {new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1,
            1,
            1,
            retryAttempts
        );
    }
}
