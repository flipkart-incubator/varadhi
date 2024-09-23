package com.flipkart.varadhi.qos.entity;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.Converter;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Suppression data for rate limiting. Stores suppression factor for each topic.
 */
@Getter
@Setter
public class SuppressionData<T> {
    @JsonDeserialize(converter = HashMapTransformer.class)
    Map<String, T> suppressionFactor;

    public SuppressionData() {
        this.suppressionFactor = new ConcurrentHashMap<>();
    }

    private static class HashMapTransformer implements Converter<Map<String, SuppressionFactor>, Map<String, SuppressionFactor>> {

        @Override
        public Map<String, SuppressionFactor> convert(Map<String, SuppressionFactor> map) {
            return new HashMap<>(map);
        }

        @Override
        public JavaType getInputType(TypeFactory typeFactory) {
            return typeFactory.constructMapType(Map.class, String.class, SuppressionFactor.class);
        }

        @Override
        public JavaType getOutputType(TypeFactory typeFactory) {
            return typeFactory.constructMapType(Map.class, String.class, SuppressionFactor.class);
        }

    }
}
