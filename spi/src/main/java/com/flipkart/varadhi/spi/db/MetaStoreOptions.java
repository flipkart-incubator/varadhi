package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.spi.ConfigFile;
import jakarta.validation.constraints.NotBlank;
import lombok.With;

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
@With
public record MetaStoreOptions(@NotBlank (message = "providerClassName must not be blank")
String providerClassName, @NotBlank (message = "configFile must not be blank") @ConfigFile
String configFile) {
}
