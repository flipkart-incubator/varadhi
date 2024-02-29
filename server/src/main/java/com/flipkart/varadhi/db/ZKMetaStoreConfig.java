package com.flipkart.varadhi.db;


import com.flipkart.varadhi.config.ZookeeperConnectConfig;
import lombok.Data;

@Data
public class ZKMetaStoreConfig {
    private ZookeeperConnectConfig zookeeperOptions;
}
