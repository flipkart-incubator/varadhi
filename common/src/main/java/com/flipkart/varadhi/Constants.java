package com.flipkart.varadhi;

public class Constants {
    public static final int RANDOM_PARTITION_KEY_LENGTH = 5;
    public static final String API_CONTEXT_KEY = "apiContext";

    public static class PathParams {
        public static final String REQUEST_PATH_PARAM_ORG = "org";
        public static final String REQUEST_PATH_PARAM_TEAM = "team";
        public static final String REQUEST_PATH_PARAM_PROJECT = "project";
        public static final String REQUEST_PATH_PARAM_TOPIC = "topic";
    }

    public static class Tags {
        public static final String TAG_NAME_REGION = "region";
        public static final String TAG_NAME_ORG = "org";
        public static final String TAG_NAME_TEAM = "team";
        public static final String TAG_NAME_PROJECT = "project";
        public static final String TAG_NAME_TOPIC = "topic";
        public static final String TAG_NAME_IDENTITY = "identity";
        public static final String TAG_NAME_HOST = "host";
        public static final String TAG_NAME_PRODUCE_RESULT = "result";
        public static final String TAG_VALUE_RESULT_SUCCESS = "success";
        public static final String TAG_VALUE_RESULT_FAILED = "failed";
    }

    public static class HttpCodes {
        public static final int HTTP_RATE_LIMITED = 429;
        public static final int HTTP_UNPROCESSABLE_ENTITY = 422;
    }

    public static class Meters {
        public static class Produce {
            public static final String BYTES_METER = "produce.bytes";
            public static final String LATENCY_METER = "produce.latency";
        }
    }

    public static class REST_DEFAULTS {
        public static final int PAYLOAD_SIZE_MAX = 5 * 1024 * 1024;
        public static final int HEADERS_ALLOWED_MAX = 10;
        public static final int HEADER_NAME_SIZE_MAX = 64;
        public static final int HEADER_VALUE_SIZE_MAX = 256;
        public static final String DEFAULT_ORG = "default";
        public static final String DEFAULT_TEAM = "public";
        public static final String DEFAULT_PROJECT = "public";

    }
}
