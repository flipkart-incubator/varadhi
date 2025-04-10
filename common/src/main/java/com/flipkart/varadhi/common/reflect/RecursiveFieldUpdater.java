package com.flipkart.varadhi.common.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.function.BiFunction;

public class RecursiveFieldUpdater {

    public static <T, V> void visit(
        T object,
        Class<? extends Annotation> annotationClass,
        BiFunction<String, V, V> updateFunction
    ) {
        Objects.requireNonNull(object, "Object cannot be null");
        traverseObject(object, annotationClass, updateFunction, "");
    }

    private static <T, V> void traverseObject(
        T obj,
        Class<? extends Annotation> annotationClass,
        BiFunction<String, V, V> updateFunction,
        String path
    ) {
        if (obj == null)
            return;

        Class<?> clazz = obj.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String fieldPath = path.isEmpty() ? field.getName() : path + "." + field.getName();

            try {
                Object fieldValue = field.get(obj);

                // If the field is annotated with the specified annotation, process it
                if (field.isAnnotationPresent(annotationClass)) {
                    // Apply the update function to get the new value
                    V newValue = updateFunction.apply(fieldPath, (V)fieldValue);
                    setFieldValue(obj, field, newValue);
                } else if (!field.getType().isPrimitive() && field.getType()
                                                                  .getName()
                                                                  .startsWith("com.flipkart.varadhi.")) {
                    // Recursively visit nested POJOs
                    traverseObject(fieldValue, annotationClass, updateFunction, fieldPath);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to access field: " + field.getName(), e);
            }
        }
    }

    private static void setFieldValue(Object obj, Field field, Object newValue) {
        try {
            field.set(obj, newValue);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to set field value for: " + field.getName(), e);
        }
    }
}
