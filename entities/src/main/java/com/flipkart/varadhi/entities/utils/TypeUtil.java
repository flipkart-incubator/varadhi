package com.flipkart.varadhi.entities.utils;

public class TypeUtil {
    public static <T> T safeCast(Object obj, Class<T> clazz) {
        if (clazz.isAssignableFrom(obj.getClass())) {
            return clazz.cast(obj);
        } else {
            throw new IllegalArgumentException(
                "Wrong object provided. Expected " + clazz.getName() + " but got " + obj.getClass().getName()
            );
        }
    }
}
