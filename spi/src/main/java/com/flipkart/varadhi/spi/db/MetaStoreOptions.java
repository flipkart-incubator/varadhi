package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.spi.ConfigFile;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration options for metadata storage providers.
 * <p>
 * This record defines the configuration parameters required to initialize
 * a metadata store provider. It includes:
 * <ul>
 *     <li>The provider class name for dynamic loading</li>
 *     <li>The configuration file path for provider-specific settings</li>
 * </ul>
 * <p>
 * Example configuration:
 * <pre>
 * metaStore:
 *   providerClassName: com.flipkart.varadhi.db.ZookeeperProvider
 *   configFile: config/zk-config.yaml
 * </pre>
 *
 * @see MetaStoreProvider
 * @see ConfigFile
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetaStoreOptions {

    /**
     * The fully qualified class name of the metadata store provider implementation.
     */
    @NotBlank (message = "providerClassName must not be blank")
    private String providerClassName;

    /**
     * The path to the configuration file containing provider-specific settings.
     */
    @NotBlank (message = "configFile must not be blank")
    @ConfigFile
    private String configFile;
}
