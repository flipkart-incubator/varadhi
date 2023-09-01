package com.flipkart.varadhi;

public class Constants {
    public static int INITIAL_VERSION = 0;
    public static String NAME_SEPARATOR = ".";

    public static class PathParams {
        public static String REQUEST_PATH_PARAM_PROJECT = "project";
        public static String REQUEST_PATH_PARAM_TOPIC = "topic";
        public static String ORG_PATH_PARAM = "org";
        public static String TEAM_PATH_PARAM = "team";
        public static String PROJECT_PATH_PARAM = "project";
    }

    public static class Tags {
        public static String TAG_NAME_REGION = "region";
        public static String TAG_NAME_PROJECT = "project";
        public static String TAG_NAME_TOPIC = "topic";
        public static String TAG_NAME_IDENTITY = "identity";
    }

    public static class HttpCodes {
        public static int HTTP_RATE_LIMITED = 429;
        public static int HTTP_UNPROCESSABLE_ENTITY = 422;
    }
}
