package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.ZookeeperConnectConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * Configuration class for ZooKeeper-based metadata store.
 * This class encapsulates the configuration parameters required for establishing
 * and maintaining a connection to the ZooKeeper ensemble.
 *
 * <p>The configuration includes:
 * <ul>
 *     <li>ZooKeeper connection options</li>
 *     <li>Any additional metadata store specific configurations</li>
 * </ul>
 *
 * @see com.flipkart.varadhi.common.ZookeeperConnectConfig
 * @see ZookeeperProvider
 */
@Data
public class ZKMetaStoreConfig {
    @NotNull
    @Valid
    private ZookeeperConnectConfig globalZookeeperOptions;

    @NotNull
    @Valid
    private ZookeeperConnectConfig localZookeeperOptions;
}
