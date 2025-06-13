package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.*;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.entities.Samples.PROJECT_1;
import static com.flipkart.varadhi.entities.Samples.U_TOPIC_RESOURCE_1;
import static com.flipkart.varadhi.entities.SubscriptionTestUtils.createSubscriptionResource;
import static org.junit.jupiter.api.Assertions.*;

class SubscriptionResourceTest {

    String PROJECT_NAME = "project1";
    String TOPIC_NAME = "topic1";
    String SUB_NAME = "subscription1";
    String DESCRIPTION = "Description";

    @Test
    void of_CreatesSubscriptionResource() {

        var subscriptionResource = createSubscriptionResource("subscription1", PROJECT_1, U_TOPIC_RESOURCE_1);

        // Basic creation check with default values
        assertAll(
            () -> assertEquals("subscription1", subscriptionResource.getName()),
            () -> assertEquals(PROJECT_1.getName(), subscriptionResource.getProject()),
            () -> assertEquals(U_TOPIC_RESOURCE_1.getName(), subscriptionResource.getTopic()),
            () -> assertEquals(U_TOPIC_RESOURCE_1.getProject(), subscriptionResource.getTopicProject()),
            () -> assertEquals("Description", subscriptionResource.getDescription()),
            () -> assertFalse(subscriptionResource.isGrouped()),
            () -> assertNotNull(subscriptionResource.getEndpoint()),
            () -> assertNotNull(subscriptionResource.getRetryPolicy()),
            () -> assertNotNull(subscriptionResource.getConsumptionPolicy()),
            () -> assertNotNull(subscriptionResource.getProperties()),
            () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, subscriptionResource.getActionCode())
        );

        // copy
        SubscriptionResource copiedResource = SubscriptionResource.of(
            subscriptionResource.getName(),
            subscriptionResource.getProject(),
            subscriptionResource.getTopic(),
            subscriptionResource.getTopicProject(),
            subscriptionResource.getDescription(),
            subscriptionResource.isGrouped(),
            subscriptionResource.getEndpoint(),
            subscriptionResource.getRetryPolicy(),
            subscriptionResource.getConsumptionPolicy(),
            null,
            subscriptionResource.getActionCode()
        );

        assertTrue(copiedResource.getProperties().isEmpty());
        assertEquals("project1.subscription1", subscriptionResource.getSubscriptionInternalName());
        assertEquals("project1.subscription1", copiedResource.getSubscriptionInternalName());
    }

    @Test
    void from_CreatesSubscriptionResourceFromVaradhiSubscription() {
        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT_NAME + "." + TOPIC_NAME,
            TOPIC_NAME,
            false,
            new TopicCapacityPolicy(100, 400, 2),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        VaradhiSubscription varadhiSubscription = SubscriptionTestUtils.builder()
                                                                       .setDescription(DESCRIPTION)
                                                                       .setGrouped(true)
                                                                       .build(
                                                                           PROJECT_NAME + "." + SUB_NAME,
                                                                           PROJECT_NAME,
                                                                           topic.getName()
                                                                       );

        SubscriptionResource subscriptionResource = SubscriptionResource.from(varadhiSubscription);

        assertAll(
            () -> assertEquals(SUB_NAME, subscriptionResource.getName()),
            () -> assertEquals(PROJECT_NAME, subscriptionResource.getProject()),
            () -> assertEquals(TOPIC_NAME, subscriptionResource.getTopic()),
            () -> assertEquals(PROJECT_NAME, subscriptionResource.getTopicProject()),
            () -> assertEquals(varadhiSubscription.getDescription(), subscriptionResource.getDescription()),
            () -> assertEquals(varadhiSubscription.isGrouped(), subscriptionResource.isGrouped()),
            () -> assertEquals(varadhiSubscription.getEndpoint(), subscriptionResource.getEndpoint()),
            () -> assertEquals(varadhiSubscription.getRetryPolicy(), subscriptionResource.getRetryPolicy()),
            () -> assertEquals(varadhiSubscription.getConsumptionPolicy(), subscriptionResource.getConsumptionPolicy()),
            () -> assertEquals(varadhiSubscription.getProperties(), subscriptionResource.getProperties()),
            () -> assertEquals(varadhiSubscription.getStatus().getActionCode(), subscriptionResource.getActionCode())
        );
    }

    @Test
    void buildInternalName_ReturnsCorrectInternalName() {
        String internalName = SubscriptionResource.buildInternalName("a", "b");
        assertEquals("a.b", internalName);
    }
}
