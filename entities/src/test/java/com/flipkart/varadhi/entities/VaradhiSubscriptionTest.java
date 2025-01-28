package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VaradhiSubscriptionTest {

    private static final Endpoint DEFAULT_ENDPOINT = new Endpoint.HttpEndpoint(
            URI.create("http://localhost:8080"),
            "GET", "", 500, 500, false
    );

    private static final RetryPolicy DEFAULT_RETRY_POLICY = new RetryPolicy(
            new CodeRange[]{new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1, 1, 1, 3
    );

    private static final ConsumptionPolicy DEFAULT_CONSUMPTION_POLICY = new ConsumptionPolicy(
            10, 1, 1,
            false, 1, null
    );

    private static final TopicCapacityPolicy DEFAULT_CAPACITY_POLICY = new TopicCapacityPolicy(
            1, 10, 1);

    private static final SubscriptionShards DEFAULT_SHARDS = new SubscriptionUnitShard(
            0, DEFAULT_CAPACITY_POLICY, null, null, null);

    @Test
    void createSubscription_Success() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        assertEquals("sub1", subscription.getName());
        assertEquals("project1", subscription.getProject());
        assertEquals("topic1", subscription.getTopic());
        assertEquals("description", subscription.getDescription());
        assertTrue(subscription.isGrouped());
        assertNotNull(subscription.getEndpoint());
        assertNotNull(subscription.getRetryPolicy());
        assertNotNull(subscription.getConsumptionPolicy());
        assertNotNull(subscription.getShards());
        assertNotNull(subscription.getProperties());
    }

    @Test
    void createSubscription_InvalidProject_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> {
                    VaradhiSubscription.of(
                            "sub1", "", "topic1", "description", true,
                            DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                            DEFAULT_SHARDS, Map.of("key", "value")
                    );
                }
        );

        assertEquals("Project cannot be null or empty", exception.getMessage());
    }

    @Test
    void createSubscription_InvalidTopic_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> {
                    VaradhiSubscription.of(
                            "sub1", "project1", "", "description", true,
                            DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                            DEFAULT_SHARDS, Map.of("key", "value")
                    );
                }
        );

        assertEquals("Topic cannot be null or empty", exception.getMessage());
    }

    @Test
    void createSubscription_NullShards_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> {
                    VaradhiSubscription.of(
                            "sub1", "project1", "topic1", "description", true,
                            DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                            null, Map.of("key", "value")
                    );
                }
        );

        assertEquals("Shards cannot be null or empty", exception.getMessage());
    }

    @Test
    void createSubscription_NullProperties_ThrowsException() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> {
                    VaradhiSubscription.of(
                            "sub1", "project1", "topic1", "description", true,
                            DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                            DEFAULT_SHARDS, null
                    );
                }
        );

        assertEquals("Properties cannot be null or empty", exception.getMessage());
    }

    @Test
    void markCreateFailed_ChangesStateToCreateFailed() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        subscription.markCreateFailed("Creation failed");
        assertEquals(VaradhiSubscription.State.CREATE_FAILED, subscription.getStatus().getState());
        assertEquals("Creation failed", subscription.getStatus().getMessage());
    }

    @Test
    void markCreated_ChangesStateToCreated() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        subscription.markCreated();
        assertEquals(VaradhiSubscription.State.CREATED, subscription.getStatus().getState());
        assertNull(subscription.getStatus().getMessage());
    }

    @Test
    void markDeleteFailed_ChangesStateToDeleteFailed() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        subscription.markDeleteFailed("Deletion failed");
        assertEquals(VaradhiSubscription.State.DELETE_FAILED, subscription.getStatus().getState());
        assertEquals("Deletion failed", subscription.getStatus().getMessage());
    }

    @Test
    void markDeleting_ChangesStateToDeleting() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        subscription.markDeleting();
        assertEquals(VaradhiSubscription.State.DELETING, subscription.getStatus().getState());
        assertNull(subscription.getStatus().getMessage());
    }

    @Test
    void markInactive_ChangesStateToInactive() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        subscription.markInactive();
        assertEquals(VaradhiSubscription.State.INACTIVE, subscription.getStatus().getState());
        assertNull(subscription.getStatus().getMessage());
    }

    @Test
    void restore_ChangesStateToCreated() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        subscription.markInactive();
        subscription.restore();
        assertEquals(VaradhiSubscription.State.CREATED, subscription.getStatus().getState());
        assertEquals("Entity restored to created state.", subscription.getStatus().getMessage());
    }

    @Test
    void getIntProperty_ReturnsCorrectValue() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "10")
        );

        assertEquals(10, subscription.getIntProperty("key"));
    }

    @Test
    void getIntProperty_PropertyNotFound_ThrowsException() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "value")
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> {
                    subscription.getIntProperty("nonExistentKey");
                }
        );

        assertEquals("Property not found: nonExistentKey", exception.getMessage());
    }

    @Test
    void getIntProperty_InvalidValue_ThrowsException() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
                "sub1", "project1", "topic1", "description", true,
                DEFAULT_ENDPOINT, DEFAULT_RETRY_POLICY, DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS, Map.of("key", "invalid")
        );

        NumberFormatException exception = assertThrows(
                NumberFormatException.class, () -> {
                    subscription.getIntProperty("key");
                }
        );

        assertEquals("For input string: \"invalid\"", exception.getMessage());
    }
}
