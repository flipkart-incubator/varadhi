package com.flipkart.varadhi.utils;


import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.Length;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
    public void testLoadConfig_InvalidAttribute() throws IOException {
        // Create a temporary YAML config file
        Path configFile = tempDir.resolve("config.yaml");
        String yamlContent = "message: Hi There, Hello, World!";
        Files.write(configFile, yamlContent.getBytes());

        // Load the config using YamlLoader
        InvalidConfigException e = Assertions.assertThrows(
            InvalidConfigException.class,
            () -> YamlLoader.loadConfig(configFile.toString(), Config.class)
        );
        Assertions.assertEquals(
            String.format(
                "Failed to load config file: %s. message: length must be between 0 and 15",
                configFile.toString()
            ),
            e.getMessage()
        );
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

    @Test
    public void testLoadConfig_GenericConfig() throws IOException {
        Path configFile = tempDir.resolve("config.yaml");
        String yamlContent = """
            ---
            nestedGenericConf:
              k1:
              - ABC
              - XYZ
            genericConf:
            - ABC
            - XYZ
            """;
        Files.write(configFile, yamlContent.getBytes());

        // Load the config using YamlLoader
        GenericConfig config = YamlLoader.loadConfig(configFile.toString(), GenericConfig.class);

        // Verify the loaded config
        Assertions.assertNotNull(config);
        Assertions.assertTrue(config.getNestedGenericConf().containsKey("k1"));
        Assertions.assertNotNull(config.getNestedGenericConf().get("k1").get(0));
        Assertions.assertEquals("abc", config.getNestedGenericConf().get("k1").get(0).getField());

        Assertions.assertNotNull(config.getGenericConf().get(0));
        Assertions.assertEquals("abc", config.getGenericConf().get(0).getField());
    }

    public enum MyEnum {
        ABC("abc"), XYZ("xyz");

        private final String field;

        MyEnum(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }
    }


    // Sample config class for testing
    public static class Config {
        @Length (max = 15)
        private String message;

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }


    @Getter
    @Setter
    public static class GenericConfig {
        private Map<String, List<MyEnum>> nestedGenericConf;
        private List<MyEnum> genericConf;
    }
}
