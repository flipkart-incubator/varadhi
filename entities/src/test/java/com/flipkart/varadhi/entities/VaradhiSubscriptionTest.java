package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VaradhiSubscriptionTest {

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

    @Test
    void createSubscription_Success() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        assertAll(
            () -> assertEquals("sub1", subscription.getName()),
            () -> assertEquals("project1", subscription.getProject()),
            () -> assertEquals("topic1", subscription.getTopic()),
            () -> assertEquals("description", subscription.getDescription()),
            () -> assertTrue(subscription.isGrouped()),
            () -> assertNotNull(subscription.getEndpoint()),
            () -> assertNotNull(subscription.getRetryPolicy()),
            () -> assertNotNull(subscription.getConsumptionPolicy()),
            () -> assertNotNull(subscription.getShards()),
            () -> assertNotNull(subscription.getProperties()),
            () -> assertEquals(LifecycleStatus.State.CREATING, subscription.getStatus().getState())
        );
    }

    @Test
    void createSubscription_InvalidProject_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            VaradhiSubscription.of(
                "sub1",
                "",
                "topic1",
                "description",
                true,
                DEFAULT_ENDPOINT,
                DEFAULT_RETRY_POLICY,
                DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS,
                Map.of("key", "value"),
                LifecycleStatus.ActionCode.SYSTEM_ACTION
            );
        });

        assertEquals("Project cannot be null or empty", exception.getMessage());
    }

    @Test
    void createSubscription_InvalidTopic_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            VaradhiSubscription.of(
                "sub1",
                "project1",
                "",
                "description",
                true,
                DEFAULT_ENDPOINT,
                DEFAULT_RETRY_POLICY,
                DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS,
                Map.of("key", "value"),
                LifecycleStatus.ActionCode.SYSTEM_ACTION
            );
        });

        assertEquals("Topic cannot be null or empty", exception.getMessage());
    }

    @Test
    void createSubscription_NullShards_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            VaradhiSubscription.of(
                "sub1",
                "project1",
                "topic1",
                "description",
                true,
                DEFAULT_ENDPOINT,
                DEFAULT_RETRY_POLICY,
                DEFAULT_CONSUMPTION_POLICY,
                null,
                Map.of("key", "value"),
                LifecycleStatus.ActionCode.SYSTEM_ACTION
            );
        });

        assertEquals("Shards cannot be null or empty", exception.getMessage());
    }

    @Test
    void createSubscription_NullProperties_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            VaradhiSubscription.of(
                "sub1",
                "project1",
                "topic1",
                "description",
                true,
                DEFAULT_ENDPOINT,
                DEFAULT_RETRY_POLICY,
                DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS,
                null,
                LifecycleStatus.ActionCode.SYSTEM_ACTION
            );
        });

        assertEquals("Properties cannot be null or empty", exception.getMessage());
    }

    @Test
    void createSubscription_EmptyProperties_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            VaradhiSubscription.of(
                "sub1",
                "project1",
                "topic1",
                "description",
                true,
                DEFAULT_ENDPOINT,
                DEFAULT_RETRY_POLICY,
                DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS,
                Map.of(),
                LifecycleStatus.ActionCode.SYSTEM_ACTION
            );
        });

        assertEquals("Properties cannot be null or empty", exception.getMessage());
    }

    @Test
    void markCreateFailed_ChangesStateToCreateFailed() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        subscription.markCreateFailed("Creation failed");
        assertAll(
            () -> assertEquals(LifecycleStatus.State.CREATE_FAILED, subscription.getStatus().getState()),
            () -> assertEquals("Creation failed", subscription.getStatus().getMessage())
        );
    }

    @Test
    void markCreated_ChangesStateToCreated() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        subscription.markCreated();
        assertAll(
            () -> assertEquals(LifecycleStatus.State.CREATED, subscription.getStatus().getState()),
            () -> assertEquals("Successfully created.", subscription.getStatus().getMessage())
        );
    }

    @Test
    void markDeleteFailed_ChangesStateToDeleteFailed() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        subscription.markDeleteFailed("Deletion failed");
        assertAll(
            () -> assertEquals(LifecycleStatus.State.DELETE_FAILED, subscription.getStatus().getState()),
            () -> assertEquals("Deletion failed", subscription.getStatus().getMessage())
        );
    }

    @Test
    void markDeleting_ChangesStateToDeleting() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        subscription.markDeleting(LifecycleStatus.ActionCode.SYSTEM_ACTION, "Deleting");
        assertAll(
            () -> assertEquals(LifecycleStatus.State.DELETING, subscription.getStatus().getState()),
            () -> assertEquals("Deleting", subscription.getStatus().getMessage())
        );
    }

    @Test
    void markInactive_ChangesStateToInactive() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        subscription.markInactive(LifecycleStatus.ActionCode.SYSTEM_ACTION, "Inactive");
        assertAll(
            () -> assertEquals(LifecycleStatus.State.INACTIVE, subscription.getStatus().getState()),
            () -> assertEquals("Inactive", subscription.getStatus().getMessage())
        );
    }

    @Test
    void restore_ChangesStateToCreated() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        subscription.markInactive(LifecycleStatus.ActionCode.SYSTEM_ACTION, "Inactive");
        subscription.restore(LifecycleStatus.ActionCode.SYSTEM_ACTION, "Restored");
        assertAll(
            () -> assertEquals(LifecycleStatus.State.CREATED, subscription.getStatus().getState()),
            () -> assertEquals("Restored", subscription.getStatus().getMessage())
        );
    }

    @Test
    void getIntProperty_ReturnsCorrectValue() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "10"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        assertEquals(10, subscription.getIntProperty("key"));
    }

    @Test
    void getIntProperty_PropertyNotFound_ThrowsException() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "value"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            subscription.getIntProperty("nonExistentKey");
        });

        assertEquals("Property not found: nonExistentKey", exception.getMessage());
    }

    @Test
    void getIntProperty_InvalidValue_ThrowsException() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", "invalid"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        NumberFormatException exception = assertThrows(NumberFormatException.class, () -> {
            subscription.getIntProperty("key");
        });

        assertEquals("For input string: \"invalid\"", exception.getMessage());
    }

    @Test
    void getIntProperty_IntegerOverflow_ThrowsException() {
        VaradhiSubscription subscription = VaradhiSubscription.of(
            "sub1",
            "project1",
            "topic1",
            "description",
            true,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("key", Integer.MAX_VALUE + "1"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        assertThrows(NumberFormatException.class, () -> subscription.getIntProperty("key"));
    }
}
