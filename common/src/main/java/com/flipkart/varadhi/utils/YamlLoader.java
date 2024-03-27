package com.flipkart.varadhi.utils;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.varadhi.entities.Validator;
import com.flipkart.varadhi.exceptions.InvalidConfigException;

import java.io.File;
import java.util.List;

public class YamlLoader {

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    static {
        mapper.findAndRegisterModules();
    }

    public static <T> T loadConfig(String configFile, Class<T> clazz) {
        T config;
        try {
            config = mapper.readValue(new File(configFile), clazz);
        } catch (Exception e) {
            throw new InvalidConfigException(
                    String.format("Failed to load config file: %s as %s.", configFile, clazz), e);
        }
        List<String> failures = Validator.validate(config);
        if (failures.isEmpty()) {
            return config;
        }
        throw new InvalidConfigException(
                String.format("Failed to load config file: %s. %s", configFile, String.join(",", failures)));
    }
}
