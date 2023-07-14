package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.VaradhiEntityType;

public class ZKPathUtils {

    private static final String VARADHI_TOPIC_NAME = "VaradhiTopic";
    private static final String TOPIC_RESOURCE_NAME = "TopicResource";
    private static final String BASE_PATH = "/varadhi/entities";

    public static String getVaradhiTopicPath(String topicName) {
        return constructPath(VARADHI_TOPIC_NAME, topicName);
    }

    public static String getVaradhiEntityPath(VaradhiEntityType varadhiEntityType, String entityName) {
        return constructPath(varadhiEntityType.name(), entityName);
    }

    public static String getTopicResourceFQDN(String projectName, String topicName) {
        return String.join("/", projectName, topicName);
    }

    public static String getTopicResourcePath(String projectName, String topicName) {
        return constructPath(TOPIC_RESOURCE_NAME, getTopicResourceFQDN(projectName, topicName));
    }

    public static String constructPath(String... components) {
        return String.join("/", BASE_PATH, String.join("/", components));
    }
}
