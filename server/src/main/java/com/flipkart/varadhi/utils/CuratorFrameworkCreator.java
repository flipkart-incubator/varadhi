package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.config.ZookeeperConnectConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

@Slf4j
public class CuratorFrameworkCreator {
    
    public static CuratorFramework create(ZookeeperConnectConfig config) {
        //TODO:: Close on below
        // 1. Retry policy need to be configurable ?
        // 2. Need for ZKClientConfig while creating CuratorFramework.
        RetryPolicy retryPolicy = new RetryForever(1000);
        CuratorFramework zkCurator = CuratorFrameworkFactory.builder()
                .namespace(config.getNamespace())
                .connectString(config.getConnectUrl())
                .connectionTimeoutMs(config.getConnectTimeoutMs())
                .sessionTimeoutMs(config.getSessionTimeoutMs())
                .retryPolicy(retryPolicy)
                .build();

        zkCurator.start();

        try {
            if (!zkCurator.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
                throw new RuntimeException("Failed to connect to zookeeper within connectTimeout");
            }
        } catch (InterruptedException e) {
            zkCurator.close();
            throw new RuntimeException("Interrupted while waiting for connection to zookeeper", e);
        }
        return zkCurator;
    }
}
