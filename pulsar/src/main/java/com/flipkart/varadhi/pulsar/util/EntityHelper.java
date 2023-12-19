package com.flipkart.varadhi.pulsar.util;

import static com.flipkart.varadhi.Constants.PATH_SEPARATOR;

public class EntityHelper {
    public static String getNamespace(String tenantName, String projectName) {
        return String.join(PATH_SEPARATOR, tenantName, projectName);
    }
}
