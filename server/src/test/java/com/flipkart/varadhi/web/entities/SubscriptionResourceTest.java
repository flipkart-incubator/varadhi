package com.flipkart.varadhi.web.entities;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.web.admin.SubscriptionTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SubscriptionResourceTest extends SubscriptionTestBase {

    @Test
    void of_CreatesSubscriptionResource() {
        SubscriptionResource subscriptionResource = createSubscriptionResource(
                "subscriptionName", PROJECT, TOPIC_RESOURCE
        );
        assertAll(
                () -> assertEquals("subscriptionName", subscriptionResource.getName()),
                () -> assertEquals(PROJECT.getName(), subscriptionResource.getProject()),
                () -> assertEquals(TOPIC_RESOURCE.getName(), subscriptionResource.getTopic()),
                () -> assertEquals(TOPIC_RESOURCE.getProject(), subscriptionResource.getTopicProject()),
                () -> assertEquals("Description", subscriptionResource.getDescription()),
                () -> assertFalse(subscriptionResource.isGrouped()),
                () -> assertNotNull(subscriptionResource.getEndpoint()),
                () -> assertNotNull(subscriptionResource.getRetryPolicy()),
                () -> assertNotNull(subscriptionResource.getConsumptionPolicy()),
                () -> assertNotNull(subscriptionResource.getProperties()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, subscriptionResource.getActorCode())
        );
    }

    @Test
    void from_CreatesSubscriptionResourceFromVaradhiSubscription() {
        VaradhiSubscription varadhiSubscription = createUngroupedSubscription(
                "subscriptionName", PROJECT, VaradhiTopic.of(
                        "project1.topic1", "topic1", false, Constants.DEFAULT_TOPIC_CAPACITY,
                        LifecycleStatus.ActorCode.SYSTEM_ACTION
                )
        );
        SubscriptionResource subscriptionResource = SubscriptionResource.from(varadhiSubscription);
        assertAll(
                () -> assertEquals("subscriptionName", subscriptionResource.getName()),
                () -> assertEquals(PROJECT.getName(), subscriptionResource.getProject()),
                () -> assertEquals("topic1", subscriptionResource.getTopic()),
                () -> assertEquals("project1", subscriptionResource.getTopicProject()),
                () -> assertEquals(varadhiSubscription.getDescription(), subscriptionResource.getDescription()),
                () -> assertFalse(subscriptionResource.isGrouped()),
                () -> assertNotNull(subscriptionResource.getEndpoint()),
                () -> assertNotNull(subscriptionResource.getRetryPolicy()),
                () -> assertNotNull(subscriptionResource.getConsumptionPolicy()),
                () -> assertNotNull(subscriptionResource.getProperties()),
                () -> assertEquals(LifecycleStatus.ActorCode.SYSTEM_ACTION, subscriptionResource.getActorCode())
        );
    }

    @Test
    void buildInternalName_ReturnsCorrectInternalName() {
        String internalName = SubscriptionResource.buildInternalName(PROJECT.getName(), "subscriptionName");
        assertEquals("project1.subscriptionName", internalName);
    }

    @Test
    void getSubscriptionInternalName_ReturnsCorrectInternalName() {
        SubscriptionResource subscriptionResource = createSubscriptionResource(
                "subscriptionName", PROJECT, TOPIC_RESOURCE
        );
        assertEquals("project1.subscriptionName", subscriptionResource.getSubscriptionInternalName());
    }
}
