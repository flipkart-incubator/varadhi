package com.flipkart.varadhi.pulsar.util;

import static com.flipkart.varadhi.Constants.PULSAR_PATH_SEPARATOR;

public class EntityHelper {
    public static String getNamespace(String tenantName, String projectName) {
        return String.join(PULSAR_PATH_SEPARATOR, tenantName, projectName);
    }
}
