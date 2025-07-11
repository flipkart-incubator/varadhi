package com.flipkart.varadhi.common;

import com.flipkart.varadhi.entities.TopicCapacityPolicy;

/**
 * Constants class contains various constant values used throughout the application.
 * This class is not meant to be instantiated.
 */
public class Constants {
    // Length of the random partition key
    public static final int RANDOM_PARTITION_KEY_LENGTH = 5;

    // TODO: This header is intended for testing purposes only. The "x_" prefix may result in it being sent to the destination during consumption.
    // Header for user ID, intended for testing purposes only
    public static final String USER_ID_HEADER = "x_user_id";

    // Default topic capacity policy
    public static final TopicCapacityPolicy DEFAULT_TOPIC_CAPACITY = new TopicCapacityPolicy(100, 400, 2);
    // System identity constant
    public static final String SYSTEM_IDENTITY = "System";
    public static final String ANONYMOUS_IDENTITY = "Anonymous";

    // Handler name for entity events.
    public static final String ENTITY_EVENTS_HANDLER = "entity-events";

    private Constants() {
        // Private constructor to prevent instantiation
    }

    /**
     * PathParams class contains constants for various path parameters.
     * This class is not meant to be instantiated.
     */
    public static class PathParams {
        public static final String PATH_PARAM_ORG = "org";
        public static final String PATH_PARAM_TEAM = "team";
        public static final String PATH_PARAM_PROJECT = "project";
        public static final String PATH_PARAM_TOPIC = "topic";
        public static final String PATH_PARAM_SUBSCRIPTION = "subscription";
        public static final String PATH_PARAM_ORG_FILTER_NAME = "orgFilterName";

        private PathParams() {
            // Private constructor to prevent instantiation
        }
    }


    public static class ContextKeys {
        public static final String USER_CONTEXT = "userContext";
        public static final String API_RESPONSE = "api-response";
        public static final String IS_SUPER_USER = "varadhi.isSuperUser";
        public static final String RESOURCE_HIERARCHY = "varadhi.resourceHierarchy";
        public static final String REQUEST_BODY = "varadhi.body";
    }


    public static class MethodNames {
        public static final String GET = "GET";
        public static final String CREATE = "CREATE";
        public static final String UPDATE = "UPDATE";
        public static final String DELETE = "DELETE";
        public static final String LIST = "LIST";

        public static final String SET = "SET";

        public static final String RESTORE = "RESTORE";

        public static final String UNSIDELINE = "UNSIDELINE";
        public static final String LIST_MESSAGES = "LIST_MESSAGES";

        public static final String START = "START";
        public static final String STOP = "STOP";

        public static final String LIST_PROJECTS = "LIST_PROJECTS";

        public static final String PRODUCE = "PRODUCE";

        private MethodNames() {
            // Private constructor to prevent instantiation
        }
    }


    /**
     * QueryParams class contains constants for various query parameters.
     * This class is not meant to be instantiated.
     */
    public static class QueryParams {
        public static final String QUERY_PARAM_DELETION_TYPE = "deletionType";
        public static final String QUERY_PARAM_IGNORE_CONSTRAINTS = "ignoreConstraints";
        public static final String QUERY_PARAM_INCLUDE_INACTIVE = "includeInactive";
        public static final String QUERY_PARAM_MESSAGE = "message";

        private QueryParams() {
            // Private constructor to prevent instantiation
        }
    }


    /**
     * Tags class contains constants for various tags used in the application.
     * This class is not meant to be instantiated.
     */
    public static class Tags {
        public static final String TAG_REGION = "region";
        public static final String TAG_ORG = "org";
        public static final String TAG_TEAM = "team";
        public static final String TAG_PROJECT = "project";
        public static final String TAG_TOPIC = "topic";
        public static final String TAG_SUBSCRIPTION = "subscription";
        public static final String TAG_IDENTITY = "identity";
        public static final String TAG_REMOTE_HOST = "host";
        public static final String TAG_PRODUCE_RESULT = "result";
        public static final String TAG_VALUE_RESULT_SUCCESS = "success";
        public static final String TAG_VALUE_RESULT_FAILED = "failed";

        private Tags() {
            // Private constructor to prevent instantiation
        }
    }


    /**
     * HttpCodes class contains constants for various HTTP status codes.
     * This class is not meant to be instantiated.
     */
    public static class HttpCodes {
        public static final int HTTP_RATE_LIMITED = 429;
        public static final int HTTP_UNPROCESSABLE_ENTITY = 422;

        private HttpCodes() {
            // Private constructor to prevent instantiation
        }
    }


    /**
     * Meters class contains constants for various meters used in the application.
     * This class is not meant to be instantiated.
     */
    public static class Meters {

        /**
         * Produce class contains constants for produce meters.
         * This class is not meant to be instantiated.
         */
        public static class Produce {
            public static final String BYTES_METER = "produce.bytes";
            public static final String LATENCY_METER = "produce.latency";

            private Produce() {
                // Private constructor to prevent instantiation
            }
        }

        private Meters() {
            // Private constructor to prevent instantiation
        }
    }


    /**
     * RestDefaults class contains default values for REST configurations.
     * This class is not meant to be instantiated.
     */
    public static class RestDefaults {
        public static final int PAYLOAD_SIZE_MAX = 5 * 1024 * 1024;
        public static final String DEFAULT_ORG = "default";
        public static final String DEFAULT_TEAM = "public";
        public static final String DEFAULT_PROJECT = "public";

        private RestDefaults() {
            // Private constructor to prevent instantiation
        }
    }
}
