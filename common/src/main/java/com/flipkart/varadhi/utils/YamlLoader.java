package com.flipkart.varadhi.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.varadhi.exceptions.VaradhiException;

import java.io.File;

public class YamlLoader {

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    static {
        mapper.findAndRegisterModules();
    }

    public static <T> T loadConfig(String configFile, Class<T> clazz) {
        try {
            return mapper.readValue(new File(configFile), clazz);
        } catch (Exception e) {
            throw new VaradhiException(String.format("Failed to load config file: %s as %s.", configFile, clazz), e);
        }
    }
}
