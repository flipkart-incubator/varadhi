package com.flipkart.varadhi.utils;


import com.flipkart.varadhi.exceptions.VaradhiException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class YamlLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    public void testLoadConfig_ValidFile() throws IOException {
        // Create a temporary YAML config file
        Path configFile = tempDir.resolve("config.yaml");
        String yamlContent = "message: Hello, World!";
        Files.write(configFile, yamlContent.getBytes());

        // Load the config using YamlLoader
        Config config = YamlLoader.loadConfig(configFile.toString(), Config.class);

        // Verify the loaded config
        Assertions.assertNotNull(config);
        Assertions.assertEquals("Hello, World!", config.getMessage());
    }

    @Test
    public void testLoadConfig_InvalidFile() {
        // Non-existent file path
        String configFile = "nonexistent.yaml";

        // Verify that VaradhiException is thrown when loading the config
        Assertions.assertThrows(VaradhiException.class, () -> {
            YamlLoader.loadConfig(configFile, Config.class);
        });
    }

    @Test
    public void testLoadConfig_InvalidYamlContent() throws IOException {
        // Create a temporary YAML config file with invalid content
        Path configFile = tempDir.resolve("config.yaml");
        String yamlContent = "invalid_yaml_content";
        Files.write(configFile, yamlContent.getBytes());

        // Verify that VaradhiException is thrown when loading the config
        Assertions.assertThrows(VaradhiException.class, () -> {
            YamlLoader.loadConfig(configFile.toString(), Config.class);
        });
    }

    // Sample config class for testing
    public static class Config {
        private String message;

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
