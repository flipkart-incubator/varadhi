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
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        assertTrue(sub.getTargetClientIds().isEmpty());
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
            LifecycleStatus.ActionCode.SYSTEM_ACTION
        );

        String json = JsonMapper.jsonSerialize(sub);
        VaradhiSubscription deserialized = JsonMapper.jsonDeserialize(json, VaradhiSubscription.class);

        assertTrue(deserialized.getTargetClientIds().isEmpty());
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

    @Test
    void codeRange_invalidRange_fromGreaterThanTo_inRangeNeverTrue() {
        // Invalid range (500, 200): from > to is accepted but range is effectively empty
        CodeRange invalid = new CodeRange(500, 200);
        assertEquals(500, invalid.getFrom());
        assertEquals(200, invalid.getTo());
        assertFalse(invalid.inRange(200));
        assertFalse(invalid.inRange(500));
        assertFalse(invalid.inRange(350));
    }

    @Test
    void codeRange_invalidRange_deserializeFromJson_inRangeNeverTrue() {
        String invalidJson = "{\"from\": 500, \"to\": 200}";
        CodeRange deserialized = JsonMapper.jsonDeserialize(invalidJson, CodeRange.class);
        assertEquals(500, deserialized.getFrom());
        assertEquals(200, deserialized.getTo());
        assertFalse(deserialized.inRange(200));
        assertFalse(deserialized.inRange(500));
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
            LifecycleStatus.ActionCode.SYSTEM_ACTION
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
            null,
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
}
