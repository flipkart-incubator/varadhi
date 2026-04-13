package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.*;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.entities.Samples.PROJECT_1;
import static com.flipkart.varadhi.entities.Samples.U_TOPIC_RESOURCE_1;
import static com.flipkart.varadhi.entities.SubscriptionTestUtils.createSubscriptionResource;
import static org.junit.jupiter.api.Assertions.*;

class SubscriptionResourceTest {

    private static final Endpoint.HttpEndpoint DEFAULT_ENDPOINT = new Endpoint.HttpEndpoint(
        URI.create("http://localhost:8080"),
        "GET",
        "",
        500,
        500,
        false
    );

    private static final Map<String, String> DEFAULT_TARGET_CLIENT_IDS = Map.of(
        DEFAULT_ENDPOINT.getUri().toString(),
        "test"
    );

    /** Must match {@link VaradhiSubscription} validation for null/empty map. */
    private static final String TARGET_CLIENT_IDS_NULL_OR_EMPTY =
        "targetClientIds map cannot be null or empty; at least one endpoint-to-client-id mapping is required";

    private static final String TARGET_CLIENT_IDS_INVALID_ENTRY =
        "targetClientIds map keys and values must be non-null and non-blank";

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
            () -> assertTrue(subscriptionResource.getEndpointOptional().isPresent()),
            () -> assertNotNull(subscriptionResource.getRetryPolicy()),
            () -> assertNotNull(subscriptionResource.getConsumptionPolicy()),
            () -> assertNotNull(subscriptionResource.getProperties()),
            () -> assertEquals(LifecycleStatus.ActionCode.SYSTEM_ACTION, subscriptionResource.getActionCode()),
            () -> assertEquals(DEFAULT_TARGET_CLIENT_IDS, subscriptionResource.getTargetClientIds())
        );

        // copy
        SubscriptionResource copiedResource = SubscriptionResource.of(
            subscriptionResource.getName(),
            subscriptionResource.getProject(),
            subscriptionResource.getTopic(),
            subscriptionResource.getTopicProject(),
            subscriptionResource.getDescription(),
            subscriptionResource.isGrouped(),
            subscriptionResource.getEndpointOptional().orElse(null),
            subscriptionResource.getRetryPolicy(),
            subscriptionResource.getConsumptionPolicy(),
            null,
            subscriptionResource.getActionCode(),
            DEFAULT_TARGET_CLIENT_IDS
        );

        assertTrue(copiedResource.getProperties().isEmpty());
        assertEquals("project1.subscription1", subscriptionResource.getSubscriptionInternalName());
        assertEquals("project1.subscription1", copiedResource.getSubscriptionInternalName());
    }

    @Test
    void from_CreatesSubscriptionResourceFromVaradhiSubscription() {
        VaradhiTopic topic = VaradhiTopic.of(
            PROJECT_NAME,
            TOPIC_NAME,
            false,
            new TopicCapacityPolicy(100, 400, 2, 2),
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
            () -> assertEquals(varadhiSubscription.getEndpoint(), subscriptionResource.getEndpointOptional()),
            () -> assertEquals(varadhiSubscription.getRetryPolicy(), subscriptionResource.getRetryPolicy()),
            () -> assertEquals(varadhiSubscription.getConsumptionPolicy(), subscriptionResource.getConsumptionPolicy()),
            () -> assertEquals(varadhiSubscription.getProperties(), subscriptionResource.getProperties()),
            () -> assertEquals(varadhiSubscription.getStatus().getActionCode(), subscriptionResource.getActionCode()),
            () -> assertEquals(varadhiSubscription.getTargetClientIds(), subscriptionResource.getTargetClientIds())
        );
    }

    @Test
    void of_withTargetClientIds_singleEndpoint_vs_multipleQueueEndpoints() {
        SubscriptionResource base = createSubscriptionResource(SUB_NAME, PROJECT_1, U_TOPIC_RESOURCE_1);
        SubscriptionResource single = SubscriptionResource.of(
            base.getName(),
            base.getProject(),
            base.getTopic(),
            base.getTopicProject(),
            base.getDescription(),
            base.isGrouped(),
            base.getEndpointOptional().orElse(null),
            base.getRetryPolicy(),
            base.getConsumptionPolicy(),
            base.getProperties(),
            base.getActionCode(),
            Map.of(DEFAULT_ENDPOINT.getUri().toString(), "client-1")
        );
        assertEquals(Map.of(DEFAULT_ENDPOINT.getUri().toString(), "client-1"), single.getTargetClientIds());

        SubscriptionResource multiple = SubscriptionResource.of(
            base.getName(),
            base.getProject(),
            base.getTopic(),
            base.getTopicProject(),
            base.getDescription(),
            base.isGrouped(),
            base.getEndpointOptional().orElse(null),
            base.getRetryPolicy(),
            base.getConsumptionPolicy(),
            base.getProperties(),
            base.getActionCode(),
            Map.of("queue-ep-1", "q1", "queue-ep-2", "q2", "queue-ep-3", "q3")
        );
        assertEquals(Map.of("queue-ep-1", "q1", "queue-ep-2", "q2", "queue-ep-3", "q3"), multiple.getTargetClientIds());
    }

    @Test
    void of_blankOrNullInTargetClientIds_throws() {
        SubscriptionResource base = createSubscriptionResource(SUB_NAME, PROJECT_1, U_TOPIC_RESOURCE_1);
        assertTargetClientIdsRejected(base, null, TARGET_CLIENT_IDS_NULL_OR_EMPTY);
        assertTargetClientIdsRejected(base, Map.of(), TARGET_CLIENT_IDS_NULL_OR_EMPTY);
        assertTargetClientIdsRejected(base, Map.of("", "v"), TARGET_CLIENT_IDS_INVALID_ENTRY);
        assertTargetClientIdsRejected(base, Map.of("e", ""), TARGET_CLIENT_IDS_INVALID_ENTRY);
        assertTargetClientIdsRejected(base, Map.of("e1", "q1", "e2", ""), TARGET_CLIENT_IDS_INVALID_ENTRY);
        Map<String, String> nullKeyMap = new HashMap<>();
        nullKeyMap.put(null, "a");
        assertTargetClientIdsRejected(base, nullKeyMap, TARGET_CLIENT_IDS_INVALID_ENTRY);

        Map<String, String> nullValueMap = new HashMap<>();
        nullValueMap.put("e1", null);
        assertTargetClientIdsRejected(base, nullValueMap, TARGET_CLIENT_IDS_INVALID_ENTRY);
    }

    /**
     * {@link List#of(Object)} does not permit null elements (unrelated to targetClientIds map shape).
     */
    @Test
    void listOf_singleNullElement_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> List.of((String)null));
    }

    private static void assertTargetClientIdsRejected(
        SubscriptionResource base,
        Map<String, String> targetClientIds,
        String expectedMessage
    ) {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ofWithTargetClientIds(base, targetClientIds)
        );
        assertEquals(expectedMessage, ex.getMessage());
    }

    private static SubscriptionResource ofWithTargetClientIds(
        SubscriptionResource base,
        Map<String, String> targetClientIds
    ) {
        return SubscriptionResource.of(
            base.getName(),
            base.getProject(),
            base.getTopic(),
            base.getTopicProject(),
            base.getDescription(),
            base.isGrouped(),
            base.getEndpointOptional().orElse(null),
            base.getRetryPolicy(),
            base.getConsumptionPolicy(),
            base.getProperties(),
            base.getActionCode(),
            targetClientIds
        );
    }

    @Test
    void buildInternalName_ReturnsCorrectInternalName() {
        String internalName = SubscriptionResource.buildInternalName("a", "b");
        assertEquals("a.b", internalName);
    }
}
