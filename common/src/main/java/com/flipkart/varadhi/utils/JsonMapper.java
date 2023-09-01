package com.flipkart.varadhi.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.flipkart.varadhi.exceptions.VaradhiException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;

@Slf4j
public class JsonMapper {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        // Implication of adding -parameters compiler option.
        // 1. size of resultant class files increases.
        // 2. Possibly security information identifiable with parameter names. (this should not be concern
        // in case of Varadhi)
        // Benefit of ParameterNamesModule
        // avoid annotation cluster for jackson serialisation, i.e. (de)serialization works by default w/o annotation.
        mapper.registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static <T> String jsonSerialize(T entity) {
        try {
            return mapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            log.error("Failed to jsonSerialize({}): {}", entity, e);
            throw new VaradhiException(e);
        }
    }

    public static <T> T jsonDeserialize(String data, Class<T> clazz) {
        try {
            return mapper.readValue(data, clazz);
        } catch (JsonProcessingException e) {
            log.error("Failed to jsonDeserialize({}): {}", data, e);
            throw new VaradhiException(e);
        }
    }

    public static <R, T> R jsonDeserialize(String data, Class<? extends Collection> collectionClass, Class<T> clazz) {
        try {
            JavaType type = mapper.getTypeFactory().constructCollectionType(collectionClass, clazz);
            return (R) mapper.readValue(data, type);
        } catch (JsonProcessingException e) {
            log.error("Failed to jsonDeserialize({}): {}", data, e);
            throw new VaradhiException(e);
        }
    }
}
