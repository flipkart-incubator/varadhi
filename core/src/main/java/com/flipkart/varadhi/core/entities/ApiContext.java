package com.flipkart.varadhi.core.entities;

import java.util.HashMap;
import java.util.Map;

public class ApiContext {
    public static final String API_NAME = "apiName";
    public static final String REGION = "region";
    public static final String SERVICE_HOST = "serviceHost";

    public static final String IDENTITY = "identity";
    public static final String REQUEST_CHANNEL = "requestChannel";
    public static final String REMOTE_HOST = "remoteHost";
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";

    public static final String ORG = "org";
    public static final String TEAM = "team";
    public static final String PROJECT = "project";

    public static final String TOPIC = "topic";
    public static final String MESSAGE = "messageId";
    public static final String GROUP = "groupId";
    public static final String BYTES_RECEIVED = "bytesReceived";

    private final Map<String, Object> data = new HashMap<>();

    public <T> void put(String key, T value) {
        data.put(key, value);
    }
    public <T> T get(String key) {
        return (T)data.get(key);
    }

    public <T> T get(String key, T defaultValue) {
        if (data.containsKey(key)) {
            return (T) data.get(key);
        }
        return defaultValue;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Api=").append(data.get(API_NAME)).append(",");

        appendKey(sb, " entity=", REGION);
        appendKey(sb, ",", ORG);
        appendKey(sb, ",", TEAM);
        appendKey(sb, ",", PROJECT);
        appendKey(sb, ",", TOPIC);

        if (data.containsKey(MESSAGE)) {
            appendKey(sb, " message=", MESSAGE);
            appendKey(sb, ",", GROUP);
            appendKey(sb, ",", BYTES_RECEIVED);
        }

        appendAPILatency(sb);

        return sb.toString();
    }
    private void appendKey(StringBuilder sb, String sep, String key) {
        if (data.containsKey(key)) {
            sb.append(sep).append(data.get(key));
        }
    }
    private void appendAPILatency(StringBuilder sb) {
        long latencies = (long)data.get(END_TIME) - (long)data.get(START_TIME);
        sb.append(" lat(ms)=").append(latencies);
    }
}
