package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for delivery-contract fields on VaradhiSubscription (targetClientIds, callbackConfig)
 * and for CallbackConfig (fromJson, addRange, shouldCallback) / CodeRange serialize-deserialize.
 */
class SubscriptionTest {

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
    private static final SubscriptionShards DEFAULT_SHARDS = new SubscriptionUnitShard(
        0,
        new TopicCapacityPolicy(1, 10, 1, 2),
        null,
        null,
        null
    );

    // ---------- VaradhiSubscription with delivery / callback ----------

    @Test
    void varadhiSubscription_withDeliveryFields_setsGetters() {
        CallbackConfig callbackConfig = new CallbackConfig(Set.of(new CodeRange(200, 299), new CodeRange(500, 599)));
        VaradhiSubscription sub = VaradhiSubscription.of(
            "sub-1",
            "project1",
            "topic1",
            "desc",
            false,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("k", "v"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            List.of("c1", "c2"),
            callbackConfig
        );

        assertEquals(List.of("c1", "c2"), sub.getTargetClientIds());
        assertNotNull(sub.getCallbackConfig());
        assertEquals(2, sub.getCallbackConfig().getCodeRanges().size());
    }

    @Test
    void varadhiSubscription_withoutDeliveryFields_hasDefaults() {
        VaradhiSubscription sub = VaradhiSubscription.of(
            "sub-1",
            "project1",
            "topic1",
            "desc",
            false,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("k", "v"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            List.of("sub-1"),
            null
        );

        assertEquals(List.of("sub-1"), sub.getTargetClientIds());
        assertNull(sub.getCallbackConfig());
    }

    @Test
    void serializeDeserialize_varadhiSubscriptionWithCallbackConfigRoundTrip() {
        CallbackConfig callbackConfig = new CallbackConfig(Set.of(new CodeRange(200, 299), new CodeRange(500, 599)));
        VaradhiSubscription sub = VaradhiSubscription.of(
            "sub-1",
            "project1",
            "topic1",
            "desc",
            false,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("k", "v"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            List.of("c1", "c2"),
            callbackConfig
        );

        String json = JsonMapper.jsonSerialize(sub);
        assertNotNull(json);
        VaradhiSubscription deserialized = JsonMapper.jsonDeserialize(json, VaradhiSubscription.class);

        assertEquals(sub.getTargetClientIds(), deserialized.getTargetClientIds());
        assertNotNull(deserialized.getCallbackConfig());
        assertEquals(2, deserialized.getCallbackConfig().getCodeRanges().size());
    }

    @Test
    void serializeDeserialize_varadhiSubscriptionMinimalRoundTrip() {
        VaradhiSubscription sub = VaradhiSubscription.of(
            "sub-1",
            "project1",
            "topic1",
            "desc",
            false,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("k", "v"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            List.of("sub-1"),
            null
        );

        String json = JsonMapper.jsonSerialize(sub);
        VaradhiSubscription deserialized = JsonMapper.jsonDeserialize(json, VaradhiSubscription.class);

        assertEquals(List.of("sub-1"), deserialized.getTargetClientIds());
        assertNull(deserialized.getCallbackConfig());
    }

    // ---------- CodeRange ----------

    @Test
    void serializeDeserialize_codeRangeRoundTrip() {
        CodeRange range = new CodeRange(500, 502);
        String json = JsonMapper.jsonSerialize(range);
        CodeRange deserialized = JsonMapper.jsonDeserialize(json, CodeRange.class);
        assertEquals(range.getFrom(), deserialized.getFrom());
        assertEquals(range.getTo(), deserialized.getTo());
        assertTrue(deserialized.inRange(501));
    }

    // ---------- CallbackConfig (reference: QueueCallbackConfig) ----------

    @Test
    void serializeDeserialize_callbackConfigRoundTrip() {
        CallbackConfig config = new CallbackConfig(Set.of(new CodeRange(200, 299), new CodeRange(500, 599)));
        String json = JsonMapper.jsonSerialize(config);
        assertNotNull(json);
        CallbackConfig deserialized = JsonMapper.jsonDeserialize(json, CallbackConfig.class);
        assertNotNull(deserialized.getCodeRanges());
        assertEquals(2, deserialized.getCodeRanges().size());
        assertTrue(deserialized.shouldCallback(200));
        assertTrue(deserialized.shouldCallback(599));
        assertTrue(deserialized.shouldCallback(200));
    }

    @Test
    void callbackConfig_addRange_mutable() {
        CallbackConfig config = new CallbackConfig();
        config.addRange(new CodeRange(200, 299));
        config.addRange(new CodeRange(500, 502));
        assertEquals(2, config.getCodeRanges().size());
        assertTrue(config.shouldCallback(250));
        assertTrue(config.shouldCallback(501));
    }

    @Test
    void callbackConfig_noArgConstructor_emptyRanges() {
        CallbackConfig config = new CallbackConfig();
        assertTrue(config.getCodeRanges().isEmpty());
        assertFalse(config.shouldCallback(200));
    }

    @Test
    void varadhiSubscription_nullOrEmptyTargetClientIds_throws() {
        IllegalArgumentException nullEx = assertThrows(
            IllegalArgumentException.class,
            () -> VaradhiSubscription.of(
                "sub-1",
                "project1",
                "topic1",
                "desc",
                false,
                DEFAULT_ENDPOINT,
                DEFAULT_RETRY_POLICY,
                DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS,
                Map.of("k", "v"),
                LifecycleStatus.ActionCode.SYSTEM_ACTION,
                null,
                null
            )
        );
        assertTrue(nullEx.getMessage().contains("targetClientIds"));

        IllegalArgumentException emptyEx = assertThrows(
            IllegalArgumentException.class,
            () -> VaradhiSubscription.of(
                "sub-1",
                "project1",
                "topic1",
                "desc",
                false,
                DEFAULT_ENDPOINT,
                DEFAULT_RETRY_POLICY,
                DEFAULT_CONSUMPTION_POLICY,
                DEFAULT_SHARDS,
                Map.of("k", "v"),
                LifecycleStatus.ActionCode.SYSTEM_ACTION,
                List.of(),
                null
            )
        );
        assertTrue(emptyEx.getMessage().contains("targetClientIds"));
    }

    @Test
    void callbackConfigFactory_returnsNullWhenNoCallbackConfig() {
        VaradhiSubscription sub = VaradhiSubscription.of(
            "sub-1",
            "project1",
            "topic1",
            "desc",
            false,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("k", "v"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            List.of("sub-1"),
            null
        );
        assertNull(sub.getCallbackConfig());
    }

    @Test
    void callbackConfigFactory_returnsConfigWhenPresent() {
        CallbackConfig callbackConfig = new CallbackConfig(Set.of(new CodeRange(500, 502)));
        VaradhiSubscription sub = VaradhiSubscription.of(
            "sub-1",
            "project1",
            "topic1",
            "desc",
            false,
            DEFAULT_ENDPOINT,
            DEFAULT_RETRY_POLICY,
            DEFAULT_CONSUMPTION_POLICY,
            DEFAULT_SHARDS,
            Map.of("k", "v"),
            LifecycleStatus.ActionCode.SYSTEM_ACTION,
            List.of("c1"),
            callbackConfig
        );

        CallbackConfig config = sub.getCallbackConfig();
        assertNotNull(config);
        assertEquals(1, config.getCodeRanges().size());
        assertTrue(config.getCodeRanges().stream().anyMatch(r -> r.getFrom() == 500 && r.getTo() == 502));
        assertTrue(config.shouldCallback(500));
        assertTrue(config.shouldCallback(501));
        assertFalse(config.shouldCallback(499));
        assertFalse(config.shouldCallback(503));
    }

    @Test
    void callbackConfig_invalidCodeRange_fromGreaterThanTo_neverMatches() {
        // Invalid range: from (500) > to (200) — no code can satisfy code >= 500 && code <= 200
        CallbackConfig config = new CallbackConfig(Set.of(new CodeRange(500, 200)));
        assertNotNull(config);
        assertEquals(1, config.getCodeRanges().size());
        assertFalse(config.shouldCallback(200));
        assertFalse(config.shouldCallback(350));
        assertFalse(config.shouldCallback(500));
        assertFalse(config.shouldCallback(501));
    }

    @Test
    void callbackConfig_invalidCodeRange_reversedBounds_mixedWithValid() {
        // One valid range (200–299) and one invalid (500, 200)
        CallbackConfig config = new CallbackConfig(Set.of(new CodeRange(200, 299), new CodeRange(500, 200)));
        assertNotNull(config);
        assertEquals(2, config.getCodeRanges().size());
        assertTrue(config.shouldCallback(200));
        assertTrue(config.shouldCallback(250));
        assertFalse(config.shouldCallback(500));
        assertFalse(config.shouldCallback(350));
    }
}
