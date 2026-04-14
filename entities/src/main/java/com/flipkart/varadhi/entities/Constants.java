package com.flipkart.varadhi.entities;

import java.util.Map;

public class Constants {
    public static class SubscriptionProperties {
        public static final String UNSIDELINE_API_MESSAGE_COUNT = "unsideline.api.message_count";
        public static final String UNSIDELINE_API_GROUP_COUNT = "unsideline.api.group_count";
        public static final String GETMESSAGES_API_MESSAGES_LIMIT = "getmessages.api.messages_limit";

    }


    /**
     * Defaults for queue web resources: programmatic construction, JSON omissions (see {@code QueueResource}), and
     * {@code QueueHandlers#setRequestBody} alignment.
     */
    public static final class QueueDefaults {
        public static final boolean SECURED = true;
        public static final boolean GROUPED = true;

        /** Owning app id; {@code null} means unset. */
        public static final String APP_ID = null;

        public static final String NFR_STRATEGY = null;
        public static final String ACTIVE_PRODUCE_ZONE = null;
        public static final String NFR_FILTER_NAME = null;

        public static final LifecycleStatus.ActionCode ACTION_CODE = LifecycleStatus.ActionCode.USER_ACTION;

        public static final RetryPolicy RETRY_POLICY = new RetryPolicy(
            new CodeRange[] {new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1,
            1,
            1,
            3
        );
        public static final ConsumptionPolicy CONSUMPTION_POLICY = new ConsumptionPolicy(10, 1, 1, false, 1, null);

        /**
         * Default subscription properties when a queue body omits {@code properties} (aligned with subscription test defaults).
         */
        public static final Map<String, String> SUBSCRIPTION_PROPERTIES = Map.of();

        public static TopicCapacityPolicy defaultCapacity() {
            return TopicCapacityPolicy.getDefault();
        }

        public static Map<String, String> subscriptionPropertiesCopy() {
            return Map.copyOf(SUBSCRIPTION_PROPERTIES);
        }

        private QueueDefaults() {
        }
    }
}
