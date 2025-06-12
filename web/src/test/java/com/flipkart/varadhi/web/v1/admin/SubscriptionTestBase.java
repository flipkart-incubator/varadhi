package com.flipkart.varadhi.web.v1.admin;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.web.DlqMessage;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.core.ProjectService;
import com.flipkart.varadhi.core.SubscriptionService;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.utils.SubscriptionPropertyValidator;
import com.flipkart.varadhi.utils.VaradhiSubscriptionFactory;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.entities.web.TopicResource;
import com.google.common.collect.ArrayListMultimap;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class SubscriptionTestBase extends WebTestBase {

    private final Random random = new Random();

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
