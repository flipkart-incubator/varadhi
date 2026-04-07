package com.flipkart.varadhi.entities;

public class Constants {
    public static class SubscriptionProperties {
        public static final String UNSIDELINE_API_MESSAGE_COUNT = "unsideline.api.message_count";
        public static final String UNSIDELINE_API_GROUP_COUNT = "unsideline.api.group_count";
        public static final String GETMESSAGES_API_MESSAGES_LIMIT = "getmessages.api.messages_limit";

    }


    /**
     * Default queue subscription retry/consumption when the HTTP layer fills an omitted body (see queue admin handlers).
     */
    public static final class QueueDefaults {
        public static final RetryPolicy RETRY_POLICY = new RetryPolicy(
            new CodeRange[] {new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1,
            1,
            1,
            3
        );
        public static final ConsumptionPolicy CONSUMPTION_POLICY = new ConsumptionPolicy(10, 1, 1, false, 1, null);

        private QueueDefaults() {
        }
    }
}
