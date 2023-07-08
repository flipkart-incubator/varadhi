package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.exceptions.VaradhiException;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Path;

public class YamlLoader {

    public static <T> T loadConfig(String configFile, Class<T> clazz) {
        try {
            return new Yaml().loadAs(Files.readString(Path.of(configFile)), clazz);
        } catch (Exception e) {
            throw new VaradhiException(String.format("Failed to load config file: %s as %s.", configFile, clazz), e);
        }
    }
}
