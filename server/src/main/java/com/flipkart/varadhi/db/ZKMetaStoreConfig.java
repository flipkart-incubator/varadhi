package com.flipkart.varadhi.db;

import com.flipkart.varadhi.config.ZookeeperConnectConfig;
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
 * @see ZookeeperConnectConfig
 * @see ZookeeperProvider
 */
@Data
public class ZKMetaStoreConfig {
    private ZookeeperConnectConfig zookeeperOptions;
}
