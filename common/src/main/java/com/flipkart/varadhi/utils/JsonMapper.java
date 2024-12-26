package com.flipkart.varadhi.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.flipkart.varadhi.exceptions.VaradhiException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class JsonMapper {
    @Getter
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        // Implication of adding -parameters compiler option.
        // 1. size of resultant class files increases.
        // 2. Possibly security information identifiable with parameter names. (this should not be concern
        // in case of Varadhi)
        // Benefit of ParameterNamesModule
        // avoid annotation cluster for jackson serialization, i.e. (de)serialization works by default w/o annotation.
        mapper.registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));
        mapper.registerModule(new Jdk8Module());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
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

    public static <T> byte[] jsonSerializeAsBytes(T entity) {
        try {
            return mapper.writeValueAsBytes(entity);
        } catch (JsonProcessingException e) {
            log.error("Failed to jsonSerialize({}): {}", entity, e);
            throw new VaradhiException(e);
        }
    }

    public static <T> T jsonDeserialize(byte[] data, Class<T> clazz) {
        try {
            return mapper.readValue(data, clazz);
        } catch (IOException e) {
            log.error("Failed to jsonDeserialize({}): {}", data, e);
            throw new VaradhiException(e);
        }
    }
}
