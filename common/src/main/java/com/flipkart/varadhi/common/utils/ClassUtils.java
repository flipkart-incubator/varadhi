package com.flipkart.varadhi.common.utils;

import com.flipkart.varadhi.common.exceptions.InvalidConfigException;

public final class ClassUtils {

    private ClassUtils() {
    }

    public static <T> T loadClass(String className) {
        try {
            if (null != className && !className.isBlank()) {
                Class<T> pluginClass = (Class<T>)Class.forName(className);
                return pluginClass.getDeclaredConstructor().newInstance();
            }
            throw new InvalidConfigException("No class provided.");
        } catch (Exception e) {
            throw new InvalidConfigException(String.format("Fail to load class %s.", className), e);
        }
    }
}
